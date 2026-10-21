using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using TICO.GAUDI.Commons;

namespace IotedgeV2TransportController
{
    /// <summary>
    /// Application Main class
    /// </summary>
    internal class MyApplicationMain : IApplicationMain
    {
        static ILogger MyLogger { get; } = LoggerFactory.GetLogger(typeof(MyApplicationMain));
        private int _disposed = 0; // 0: false, 1: true

        static string InputName { get; set; } = string.Empty;

        static string OutputName { get; set; } = string.Empty;

        static bool IsTransCtrlEnabled { get; set; }

        static MessageEncoder MyEncoder { get; set; }

        /// エッジランタイムが無応答の場合にSendMessageAsyncをキャンセルするまでの待ち時間
        static TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// リソース解放
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: DisposeAsync");

            // 2重実行防止
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Exit Method: Already disposed, skipping DisposeAsync");
                return;
            }

            try
            {
                // ここでApplicationMainのリソース解放処理を行う
                // 非同期リソースがある場合は適切に解放する
                // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここから＝＝＝＝＝＝＝＝＝＝＝＝＝

                await Task.CompletedTask; // 非同期リソース解放処理をここに記述

                // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここまで＝＝＝＝＝＝＝＝＝＝＝＝＝
            }
            catch (Exception ex)
            {
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"Error in DisposeAsync: {ex.Message}", true);
                // 例外は再スローしない（Disposeでは通常例外を抑制）
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: DisposeAsync");
        }

        /// <summary>
        /// アプリケーション初期化					
        /// システム初期化前に呼び出される
        /// </summary>
        /// <returns></returns>
        public async Task<bool> InitializeAsync()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: InitializeAsync");

            // ここでApplicationMainの初期化処理を行う。
            // 通信は未接続、DesiredPropertiesなども未取得の状態
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここから＝＝＝＝＝＝＝＝＝＝＝＝＝
            bool retStatus = true;

            await Task.CompletedTask;
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここまで＝＝＝＝＝＝＝＝＝＝＝＝＝

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: InitializeAsync");
            return retStatus;
        }

        /// <summary>
        /// アプリケーション起動処理					
        /// システム初期化完了後に呼び出される
        /// </summary>
        /// <returns></returns>
        public async Task<bool> StartAsync()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: StartAsync");

            // ここでApplicationMainの起動処理を行う。
            // 通信は接続済み、DesiredProperties取得済みの状態
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここから＝＝＝＝＝＝＝＝＝＝＝＝＝
            bool retStatus = true;

            // Register callback to be called when a message is received by the module
            IApplicationEngine engine = ApplicationEngineFactory.GetEngine();
            await engine.AddMessageInputHandlerAsync(InputName, OnMessageReceivedAsync, null).ConfigureAwait(false);
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここまで＝＝＝＝＝＝＝＝＝＝＝＝＝

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: StartAsync");
            return retStatus;
        }

        /// <summary>
        /// アプリケーション解放。					
        /// </summary>
        /// <returns></returns>
        public async Task<bool> TerminateAsync()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: TerminateAsync");

            // ここでApplicationMainの終了処理を行う。
            // アプリケーション終了時や、
            // DesiredPropertiesの更新通知受信後、
            // 通信切断時の回復処理時などに呼ばれる。
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここから＝＝＝＝＝＝＝＝＝＝＝＝＝
            bool retStatus = true;

            // 終了前に累積したメッセージを送信する
            IsTransCtrlEnabled = false;
            await TransportController.FlushAndDisposeAll();
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここまで＝＝＝＝＝＝＝＝＝＝＝＝＝

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: TerminateAsync");
            return retStatus;
        }


        /// <summary>
        /// DesiredPropertis更新コールバック。					
        /// </summary>
        /// <param name="desiredProperties">DesiredPropertiesデータ。JSONのルートオブジェクトに相当。</param>
        /// <returns></returns>
        public async Task<bool> OnDesiredPropertiesReceivedAsync(JObject desiredProperties)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: OnDesiredPropertiesReceivedAsync");

            // DesiredProperties更新時の反映処理を行う。
            // 必要に応じて、メンバ変数への格納等を実施。
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここから＝＝＝＝＝＝＝＝＝＝＝＝＝
            bool retStatus = true;

            try
            {
                // MessageInput
                InputName = Util.GetDesiredProperty<string>(desiredProperties, "input", false, "input");

                // MessageOutput
                OutputName = Util.GetDesiredProperty<string>(desiredProperties, "output", false, "output");

                // 圧縮モード
                var compress = Util.GetDesiredProperty<MessageEncoder.Compress>(desiredProperties, "compress", false, MessageEncoder.Compress.NONE);

                // MessageEncoderのインスタンスを作成
                MyEncoder = new MessageEncoder(compress);

                // 転送制御機能を有効化するかどうか
                IsTransCtrlEnabled = Util.GetDesiredProperty<bool>(desiredProperties, "transportcontrol", true);

                // 転送制御機能が無効の場合、以降のプロパティは無視する
                if(IsTransCtrlEnabled)
                {
                    // 帯域制御機能を有効化するかどうか
                    bool isBandCtrlEnabled = false;
                    isBandCtrlEnabled = Util.GetDesiredProperty<bool>(desiredProperties, "bandwidthcontrol", true);

                    // メッセージバッファの最大サイズ
                    int sendSizeMax = 0;
                    sendSizeMax = Util.GetDesiredProperty<int>(desiredProperties, "sendsizemax", true, 0, condition: x => x >= 1 && x <= 256000);

                    // メッセージ送信サイクル
                    int sendCycle = 0;
                    sendCycle = Util.GetDesiredProperty<int>(desiredProperties, "sendcycle", true, 0, condition: x => x >= 1);

                    // ユニットキー
                    string strUnitKeys = null;
                    strUnitKeys = Util.GetDesiredProperty<string>(desiredProperties, "unitkey", false, null);
                    string[] unitKeys = null;
                    if (strUnitKeys == null)
                    {
                        MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property unitkey dose not exist and set single unit mode.");
                        unitKeys = null;
                    }
                    else
                    {
                        unitKeys = strUnitKeys.Split(",");
                        foreach (string s in unitKeys)
                        {
                            MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property unitkey is: {s}");
                        }
                    }

                    // TransportControllerのインスタンスを作成
                    TransportController.SetSettings(isBandCtrlEnabled, sendSizeMax, sendCycle, unitKeys);
                }
            }
            catch (Exception ex)
            {
                var errmsg = $"Property error. {ex}";
                MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Exit Method: OnDesiredPropertiesReceivedAsync caused by {errmsg}");
                retStatus = false;
                return retStatus;
            }

            await Task.CompletedTask;
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここまで＝＝＝＝＝＝＝＝＝＝＝＝＝

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: OnDesiredPropertiesReceivedAsync");

            return retStatus;
        }

        /// <summary>
        /// メッセージ受信コールバック。					
        /// </summary>
        /// <param name="inputName"></param>
        /// <param name="message"></param>
        /// <param name="userContext"></param>
        /// <returns>
        /// 受信処理成否
        ///     true : 処理成功。
        ///     false ： 処理失敗。edgeHubから再送を受ける。
        /// </returns>
        public async Task<bool> OnMessageReceivedAsync(string inputName,IotMessage message,object userContext)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: OnMessageReceivedAsync");

            // メッセージ受信時のコールバック処理を行う。
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここから＝＝＝＝＝＝＝＝＝＝＝＝＝
            bool retStatus = true;

            try
            {
                IDictionary<string, string> properties = message.GetProperties();

                byte[] messageBytes = message.GetBytes();

                MyLogger.WriteLog(ILogger.LogLevel.INFO, "1 message received.");

                // 不要な処理を回避の為、TRACEログを出力する設定か確認
                if (MyLogger.IsLogLevelToOutput(ILogger.LogLevel.TRACE))
                {
                    string messageString = Encoding.UTF8.GetString(messageBytes);
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Received Message. Body: [{messageString}]");
                }

                if (IsTransCtrlEnabled)
                {
                    // メッセージをバッファに貯める
                    var input = JsonMessage.DeserializeJsonMessage(messageBytes);
                    MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Buffering {input.RecordList.Count} records.");
                    foreach (var record in input.RecordList)
                    {
                        await TransportController.SaveMessage(JsonMessage.SerializeRecordInfoByte(record), properties);
                    }
                    retStatus = true;
                }
                else
                {
                    // メッセージを送信
                    retStatus = await SendMessage(messageBytes, properties);
                    if( retStatus == true )
                    {
                        MyLogger.WriteLog(ILogger.LogLevel.INFO, "1 message sent");
                    }
                }
            }
            catch (Exception ex)
            {
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"OnMessageReceivedAsync failed. {ex}", true);
                // 旧版(v5以前)と仕様をあわせるために、trueを設定
                retStatus = true;
            }
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここまで＝＝＝＝＝＝＝＝＝＝＝＝＝

            MyLogger.WriteLog(ILogger.LogLevel.DEBUG, $"Return status : {retStatus}");
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: OnMessageReceivedAsync");
            return retStatus;
        }

        /// OnMessageReceivedAsync/Flushから呼び出しのみ、Set/Unset不要
        /// <param name="sendKey">呼び出し元のユニットキー</param>
        public static async Task<bool> SendMessage(byte[] body, IDictionary<string, string> properties, string sendKey = null)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, "Start Method: SendMessage");

            bool retStatus = true;
            IApplicationEngine engine = ApplicationEngineFactory.GetEngine();

            try
            {
                using var cancellationTokenSource = new CancellationTokenSource(SendTimeout);
                await engine.SendMessageAsync(OutputName, MyEncoder.EncodeMessage(body, properties), cancellationTokenSource.Token).ConfigureAwait(false);
                retStatus = true;
            }
            catch (OperationCanceledException)
            {
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"SendMessage timed out. Timeout: {SendTimeout.TotalSeconds} sec.", true);
                retStatus = false;
            }
            catch (Exception ex)
            {
                var errmsg = $"SendMessage failed.";
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"{errmsg} {ex}", true);
                retStatus = false;
            }

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: SendMessage");
            return retStatus;
        }
    }
}