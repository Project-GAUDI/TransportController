namespace IotedgeV2TransportController
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
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

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
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
                if (properties.TryGetValue(key, out string val))
                {
                    sb.Append(val);
                }
                else
                {
                    // " {val}"と"{val} "を区別するため一致するキーがない場合は半角スペースとする
                    sb.Append(" ");
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

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: SaveMessage");
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

                // メッセージの保存
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
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, "Start Method: Flush");

            if (BufferedMessageSize == 0)
            {
                if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.TRACE))
                {
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Flush. Not sended.");
                }
                return;
            }

            // Body部の作成
            var jsonMsg = new JsonMessage()
            {
                RecordList = new List<JsonMessage.RecordInfo>()
            };
            foreach (var bf in BufferedMessage)
            {
                jsonMsg.RecordList.Add(JsonMessage.DeserializeRecordInfo(bf));
            }
            int cnt = jsonMsg.RecordList.Count;
            var body = JsonMessage.SerializeJsonMessageByte(jsonMsg);

            int byteLength = Buffer.ByteLength(body);
            if (byteLength > SendSizeMax)
            {
                MyLogger.WriteLog(ILogger.LogLevel.WARN, $"Send body size over. UnitKey: {myUnitKey}, BufferedMessageSize: {BufferedMessageSize}, byteLength: {byteLength}");
            }

            // メッセージの送信
            var ret = await MyApplicationMain.SendMessage(body, Properties);
            if ( ret == true )
            {
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Send 1 message. UnitKey: {myUnitKey}, RecordCount: {cnt}");

                // 累積情報の初期化
                BufferedMessage.Clear();
                Properties.Clear();
                BufferedMessageSize = 0;
            }
            else
            {
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"SendMessage failed, BufferedMessage is keep.", true);
            }

            if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.DEBUG))
            {
                MyLogger.WriteLog(ILogger.LogLevel.DEBUG, $"MessageBufs count: {MessageBufs.Count}");
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Flush");
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
        /// バッファされているメッセージの有無
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
