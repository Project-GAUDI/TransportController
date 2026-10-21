namespace IotedgeV2TransportController
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using TICO.GAUDI.Commons;

    /// <summary>
    /// メッセージバッファリングを行うクラス
    /// </summary>
    class TransportController
    {
        public static bool IsBandCtrlEnabled { get; private set; }

        public static int SendSizeMax { get; private set; }

        public static int SendCycle { get; private set; }

        public static string[] UnitKeys { get; private set; }

        public static bool SingleUnitMode { get; private set; }

        private static ConcurrentDictionary<string, SemaphoreSlim> SemSlims { get; } = new ConcurrentDictionary<string, SemaphoreSlim>();

        private static ConcurrentDictionary<string, TransportController> MessageBufs { get; } = new ConcurrentDictionary<string, TransportController>();

        private static ILogger MyLogger { get; } = LoggerFactory.GetLogger(typeof(TransportController));

        private static readonly string SINGLE_UNIT_KEY = "sunit";

        private static SemaphoreSlim DictSemSlim { get; } = new SemaphoreSlim(1, 1);

        private static readonly int headerLength = 20;    // "{"RecordList":[" + α
        private static readonly int footerLength = 5;     // "]}" + α
        private static readonly int delimiterLength = 1;  // ","
        private static readonly int sendFailureMax = 5;   // 連続送信失敗の上限回数

        private static IBufferBackupStore BackupStore { get; } = BufferBackupStoreFactory.GetStore();

        /// <summary>
        /// 起動時のバックアップ復元済みフラグ（稼働中のDesiredProperties更新での再復元・二重積みを防止）
        /// </summary>
        private static bool BackupsRestored { get; set; } = false;

        /// <summary>
        /// ディスク退避対象データ（未送信メッセージとプロパティの組）
        /// </summary>
        private class BackupData
        {
            public IDictionary<string, string> Properties { get; set; }

            public List<byte[]> Messages { get; set; }
        }

        /// <summary>
        /// 共通設定情報の格納
        /// </summary>
        /// <param name="isBandCtrlEnabled">帯域制御機能の有無</param>
        /// <param name="sendSizeMax">最大送信サイズ</param>
        /// <param name="sendCycle">送信サイクル</param>
        /// <param name="unitKeys">累積キー / 指定なし（null）の場合はすべての受信メッセージを同一ユニットで累積</param>
        public static void SetSettings(bool isBandCtrlEnabled, int sendSizeMax, int sendCycle, string[] unitKeys = null)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");

            IsBandCtrlEnabled = isBandCtrlEnabled;
            SendSizeMax = sendSizeMax;
            SendCycle = sendCycle;
            UnitKeys = unitKeys;
            SingleUnitMode = (unitKeys == null) ? true : false;
            if (SingleUnitMode)
            {
                MessageBufs.TryAdd(SINGLE_UNIT_KEY, new TransportController(SINGLE_UNIT_KEY));
            }

            // 前回終了時に退避した未送信メッセージを復元（SendCycle確定後でないとタイマー生成が壊れるためこの位置）
            // 稼働中のDesiredProperties更新で再実行すると、メモリ上に残る未送信分に対して同じ退避データが二重に積まれるため、起動直後の1回のみ実行する
            if (!BackupsRestored)
            {
                RestoreAllBackups();
                BackupsRestored = true;
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
        }

        /// <summary>
        /// Commons BufferBackupに退避されている全データを復元する
        /// </summary>
        /// SetSettingsから呼び出しのみ
        private static void RestoreAllBackups()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: RestoreAllBackups");

            try
            {
                foreach (var key in BackupStore.GetBackupKeys())
                {
                    if (MessageBufs.TryGetValue(key, out var existing))
                    {
                        existing.ApplyBackup(key);
                    }
                    else
                    {
                        var ctrl = new TransportController(key);
                        ctrl.ApplyBackup(key);
                        MessageBufs.TryAdd(key, ctrl);
                    }
                }
            }
            catch (Exception ex)
            {
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"RestoreAllBackups method failed. Exception: {ex.Message}", true);
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: RestoreAllBackups");
        }

        /// <summary>
        /// ユニットキーのロック
        /// </summary>
        /// <param name="unitKey">ユニットキー</param>
        private static async Task LockAsync(string unitKey)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: LockAsync");

            await DictSemSlim.WaitAsync();

            SemaphoreSlim sem = null;

            var status = SemSlims.TryGetValue(unitKey, out sem);
            if (false == status)
            {
                sem = new SemaphoreSlim(1, 1);
                SemSlims.TryAdd(unitKey, sem);
            }
            DictSemSlim.Release();

            await sem.WaitAsync();

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: LockAsync");
        }

        /// <summary>
        /// ユニットキーのアンロック
        /// </summary>
        /// <param name="unitKey">ユニットキー</param>
        private static async Task Unlock(string unitKey)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: Unlock");

            await DictSemSlim.WaitAsync();

            SemaphoreSlim sem = null;

            var status = SemSlims.TryGetValue(unitKey, out sem);
            if (true == status)
            {
                sem.Release();
            }
            DictSemSlim.Release();

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Unlock");
        }

        /// <summary>
        /// メッセージの累積処理
        /// </summary>
        /// <param name="body">メッセージ本文</param>
        /// <param name="properties">メッセージプロパティ</param>
        /// <returns></returns>
        public static async Task SaveMessage(byte[] body, IDictionary<string, string> properties)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: SaveMessage");

            // 同一ユニットで累積
            if (SingleUnitMode)
            {
                await LockAsync(SINGLE_UNIT_KEY);
                var tmpBuf = MessageBufs[SINGLE_UNIT_KEY];
                await tmpBuf.Add(body, properties);
                await Unlock(SINGLE_UNIT_KEY);
                if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.TRACE))
                {
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: SaveMessage. SingleUnitMode={SingleUnitMode}.");
                }
                return;
            }

            // キー値の取得
            StringBuilder sb = new StringBuilder();
            foreach (var key in UnitKeys)
            {
                sb.Append(key);
                if (properties.TryGetValue(key, out string val))
                {
                    sb.Append(val);
                }
            }
            string bufkey = sb.ToString();

            // メッセージの保存
            TransportController tc = null;

            await LockAsync(bufkey);
            if (MessageBufs.TryGetValue(bufkey, out tc))
            {
                await tc.Add(body, properties);
            }
            else
            {
                var ctrl = new TransportController(bufkey);
                await ctrl.Add(body, properties);
                MessageBufs.TryAdd(bufkey, ctrl);
            }
            await Unlock(bufkey);

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: SaveMessage. UnitKey={bufkey}.");
        }

        /// <summary>
        /// 累積済みの全メッセージを送信し、メッセージバッファをクリアする
        /// </summary>
        /// TerminateAsyncから呼び出しのみ
        public static async Task FlushAndDisposeAll()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: FlushAndDisposeAll");

            await DictSemSlim.WaitAsync();

            // タイマーを停止
            foreach (var buf in MessageBufs)
            {
                buf.Value.TimerStop();
            }

            // 累積済み全メッセージを送信
            foreach (var buf in MessageBufs)
            {
                await buf.Value.Flush().ConfigureAwait(false);

                // 送信できなかったメッセージをディスクへ退避
                buf.Value.Backup();
            }

            // メッセージバッファをクリア
            MessageBufs.Clear();

            DictSemSlim.Release();

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: FlushAndDisposeAll");
        }

        private string myUnitKey = "";

        private int BufferedMessageSize { get; set; } = 0;

        private Queue<byte[]> BufferedMessage { get; } = new Queue<byte[]>();

        private IDictionary<string, string> Properties { get; } = new Dictionary<string, string>();

        private System.Timers.Timer CycleTimer { get; } = new System.Timers.Timer(SendCycle);

        private int isCycleTimerRunning = 0; // 0: 停止中, 1: 実行中

        private int sendFailureCount = 0;

        private TransportController(string unitkey)
        {
            myUnitKey = unitkey;

            CycleTimer.Elapsed += CycleTimer_Elapsed;
            CycleTimer.AutoReset = true;
            CycleTimer.Enabled = true;
            CycleTimer.Start();
        }

        /// <summary>
        /// タイマー処理
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="elapsedEventArgs"></param>
        private async void CycleTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs elapsedEventArgs)
        {
            // 送信に時間がかかっている間に次の発火が積み上がるのを防ぐ
            if (Interlocked.Exchange(ref isCycleTimerRunning, 1) == 1)
            {
                MyLogger.WriteLog(ILogger.LogLevel.DEBUG, $"Skipped CycleTimer_Elapsed because previous cycle is still running. UnitKey={myUnitKey}");
                return;
            }

            try
            {
                await CycleTimerElapsedAsync();
            }
            finally
            {
                Interlocked.Exchange(ref isCycleTimerRunning, 0);
            }
        }

        /// <summary>
        /// タイマー処理本体
        /// </summary>
        /// CycleTimer_Elapsedから呼び出しのみ
        private async Task CycleTimerElapsedAsync()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: CycleTimer_Elapsed");

            await LockAsync(myUnitKey);
            try
            {
                if (ExistBuffer())
                {
                    await Flush();
                }
                else
                {
                    await RemoveController();
                }
            }
            finally
            {
                await Unlock(myUnitKey);
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: CycleTimer_Elapsed");
        }

        /// <summary>
        /// バッファされているメッセージの有無
        /// </summary>
        private bool ExistBuffer()
        {
            // 使用頻度がかなり高いためTRACEログには残さない
            bool rt = false;
            if (0 < BufferedMessageSize)
            {
                rt = true;
            }
            return rt;
        }

        /// <summary>
        /// メッセージ追加
        /// </summary>
        /// <param name="body">本文</param>
        /// <param name="properties">プロパティ</param>
        /// <returns></returns>
        /// SaveMessageから呼び出しのみ
        private async Task Add(byte[] body, IDictionary<string, string> properties)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: Add (UnitKey={myUnitKey})");

            try
            {
                // メッセージサイズが上限に達する場合
                if (BufferedMessageSize + body.Length + headerLength + footerLength > SendSizeMax)
                {
                    if (IsBandCtrlEnabled)
                    {
                        MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Dropped record(buffer size over).");
                        if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.TRACE))
                        {
                            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Add (UnitKey={myUnitKey})");
                        }
                        return;
                    }
                    else
                    {
                        await Flush();
                    }
                }

                // バッファに追加
                BufferedMessageSize += body.Length + delimiterLength; // "," 分加算
                BufferedMessage.Enqueue(body);

                // プロパティの更新（後勝ち）
                foreach (var prop in properties)
                {
                    if (!Properties.TryAdd(prop.Key, prop.Value))
                    {
                        Properties[prop.Key] = prop.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"Add method failed. Exception: {ex.Message}", true);
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Add (UnitKey={myUnitKey})");
        }

        /// <summary>
        /// 累積メッセージの送信
        /// </summary>
        /// <returns></returns>
        // FlushAndDisposeAll(Set/Unset不要)/CycleTimer_Elapsed(Set/Unset不要)/Addから呼び出し
        private async Task Flush()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: Flush");

            if (BufferedMessageSize == 0)
            {
                if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.TRACE))
                {
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Flush. Not sended.");
                }
                return;
            }

            // Body部の作成(SendSizeMaxを超えない最大サイズで分割送信)
            var recordList = new List<JsonMessage.RecordInfo>();
            int currentSize = headerLength + footerLength;
            int position = 0;
            while (position < BufferedMessage.Count)
            {
                var buffer = BufferedMessage.ElementAt(position);
                var record = JsonMessage.DeserializeRecordInfo(buffer);
                int recordSize = buffer.Length + delimiterLength;
                if (SendSizeMax < currentSize + recordSize && 0 < recordList.Count)
                {
                    // 分割送信を行う
                    var ret = await SendRecordList(recordList).ConfigureAwait(false);
                    if (ret == true)
                    {
                        // positionを先頭に戻す
                        position = 0;
                    }
                    else
                    {
                        // 送信失敗時は処理を終了
                        recordList.Clear();
                        break;
                    }

                    // 送信対象を初期化
                    recordList.Clear();
                    currentSize = headerLength + footerLength;

                    continue;
                }

                // 送信対象に追加
                recordList.Add(record);
                currentSize += recordSize;
                position++;
            }

            // 未送信分を送信
            if (0 <recordList.Count)
            {
                await SendRecordList(recordList).ConfigureAwait(false);
            }

            // 全バッファが送信成功した場合のみリソースをクリア
            // 送信失敗の場合は、残りのバッファとPropertiesを保持
            if (BufferedMessage.Count == 0)
            {
                // メモリ上の未送信メッセージを全て送信できたため、再起動時の重複復元を防ぐ
                BackupStore.Delete(myUnitKey);
                Properties.Clear();
                BufferedMessageSize = 0;
            }

            if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.DEBUG))
            {
                MyLogger.WriteLog(ILogger.LogLevel.DEBUG, $"MessageBufs count: {MessageBufs.Count}");
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Flush");
        }

        /// <summary>
        /// レコードリストの送信
        /// </summary>
        /// <param name="recordList">送信するレコードリスト</param>
        /// <returns>
        ///     true    : 送信成功
        ///     false   : 送信失敗
        /// </returns>
        /// Flushから呼び出しのみ
        /// レコードリスト数が1以上が保証されているためレコードリスト数のチェックは行わない
        private async Task<bool> SendRecordList(List<JsonMessage.RecordInfo> recordList)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: SendRecordList");

            bool retStatus = false;
            var jsonMsg = new JsonMessage() { RecordList = new List<JsonMessage.RecordInfo>(recordList) };
            var body = JsonMessage.SerializeJsonMessageByte(jsonMsg);
            if (SendSizeMax < body.Length)
            {
                MyLogger.WriteLog(ILogger.LogLevel.WARN, $"Send body size over. UnitKey: {myUnitKey}, BodySize: {body.Length},  SendSizeMax: {SendSizeMax}");
            }

            // メッセージ送信
            var ret = await MyApplicationMain.SendMessage(body, Properties, myUnitKey).ConfigureAwait(false);
            if (ret == true)
            {
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Send 1 message. UnitKey: {myUnitKey}, RecordCount: {recordList.Count}");

                // 送信成功分のバッファを削除
                for (int cnt = 0; cnt < recordList.Count; cnt++)
                {
                    var removed = BufferedMessage.Dequeue();
                    BufferedMessageSize -= removed.Length + delimiterLength;
                }

                sendFailureCount = 0;
                retStatus = true;
            }
            else
            {
                sendFailureCount++;
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"SendMessage failed, BufferedMessage is keep. UnitKey: {myUnitKey}, RetryCount: {sendFailureCount}/{sendFailureMax}", true);

                // 送信不能が続く間にプロセスが落ちても未送信メッセージを失わないよう、上限到達時点で退避する
                if (sendFailureMax <= sendFailureCount)
                {
                    MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"Send failure count reached the limit. UnitKey: {myUnitKey}, RetryCount: {sendFailureCount}/{sendFailureMax}");
                    Backup();
                    sendFailureCount = 0;
                }

                // 送信失敗時はバッファを削除しない
                retStatus = false;
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: SendRecordList");

            return retStatus;
        }

        /// <summary>
        /// ディスクに退避されている未送信メッセージの復元
        /// </summary>
        /// <param name="key">退避データを識別するキー</param>
        /// RestoreAllBackupsから呼び出しのみ
        private void ApplyBackup(string key)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: ApplyBackup (UnitKey={key})");

            try
            {
                var backup = BackupStore.Restore<BackupData>(key)?.FirstOrDefault();
                if (backup != null)
                {
                    foreach (var body in backup.Messages)
                    {
                        BufferedMessageSize += body.Length + delimiterLength;
                        BufferedMessage.Enqueue(body);
                    }
                    foreach (var prop in backup.Properties)
                    {
                        Properties[prop.Key] = prop.Value;
                    }
                    // 復元済みデータをメモリに取り込んだため、同じ内容が再度復元されないよう退避ファイルは削除する
                    BackupStore.Delete(key);
                    MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Restored {backup.Messages.Count} records from backup. UnitKey={key}.");
                }
            }
            catch (Exception ex)
            {
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"ApplyBackup method failed. Exception: {ex.Message}", true);
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: ApplyBackup (UnitKey={key})");
        }

        /// <summary>
        /// 未送信メッセージのディスクへの退避
        /// </summary>
        /// FlushAndDisposeAllから呼び出しのみ
        private void Backup()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: Backup (UnitKey={myUnitKey})");

            try
            {
                if (0 < BufferedMessage.Count)
                {
                    var backup = new BackupData
                    {
                        Properties = new Dictionary<string, string>(Properties),
                        Messages = new List<byte[]>(BufferedMessage),
                    };
                    BackupStore.Save(myUnitKey, new List<BackupData> { backup });
                    MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Backed up {backup.Messages.Count} records. UnitKey={myUnitKey}.");
                }
                else
                {
                    // 既に送信済みのため、前回起動時の退避データが残っていれば削除
                    BackupStore.Delete(myUnitKey);
                }
            }
            catch (Exception ex)
            {
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"Backup method failed. Exception: {ex.Message}", true);
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Backup (UnitKey={myUnitKey})");
        }

        private void TimerStop()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");

            CycleTimer.Stop();
            CycleTimer.Elapsed -= CycleTimer_Elapsed;
            CycleTimer.Dispose();

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
        }

        /// <summary>
        /// バッファされているメッセージの解放
        /// </summary>
        private async Task RemoveController()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: RemoveController");

            if (myUnitKey != SINGLE_UNIT_KEY)
            {
                TimerStop();
                MessageBufs.Remove(myUnitKey, out var mbvalue);
                await DictSemSlim.WaitAsync();
                SemSlims.Remove(myUnitKey, out var ssvalue);
                DictSemSlim.Release();
            }
            else
            {
                if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.TRACE))
                {
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Not removed controller.");
                }
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: RemoveController");
        }
    }
}
