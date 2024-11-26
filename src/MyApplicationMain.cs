using System;
using System.Collections.Generic;
using System.Text;
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

        static string InputName { get; set; } = string.Empty;

        static string OutputName { get; set; } = string.Empty;

        static bool IsTransCtrlEnabled { get; set; }

        static MessageEncoder MyEncoder { get; set; }

        public void Dispose()
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Start Method: Dispose");

            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: Dispose");
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
        /// <param name=""></param>
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

            // MessageInput
            InputName = "input";
            try
            {
                InputName = Util.GetRequiredValue<string>(desiredProperties, "input");
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property input is: {InputName}");
            }
            catch (Exception)
            {
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property input dose not exist and set \"{InputName}\"");
            }

            // MessageOutput
            OutputName = "output";
            try
            {
                OutputName = Util.GetRequiredValue<string>(desiredProperties, "output");
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property output is: {OutputName}");
            }
            catch (Exception)
            {
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property output dose not exist and set \"{OutputName}\"");
            }

            // 圧縮モード
            // Compress
            string compress = "none";
            try
            {
                compress = Util.GetRequiredValue<string>(desiredProperties, "compress");
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property compress is: {compress}");
            }
            catch (Exception)
            {
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property compress dose not exist and set \"{compress}\"");
            }

            // MessageEncoderのインスタンス作成
            MyEncoder = new MessageEncoder(compress);

            // 転送制御機能を有効化するかどうか
            try
            {
                IsTransCtrlEnabled = Util.GetRequiredValue<bool>(desiredProperties, "transportcontrol");
                MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property transportcontrol is: {IsTransCtrlEnabled}");
            }
            catch (Exception ex)
            {
                var errmsg = $"Property transportcontrol dose not exist.";
                MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"{errmsg} {ex}", true);
                MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Exit Method: OnDesiredPropertiesReceivedAsync caused by {errmsg}");
                retStatus = false;
                return retStatus;
            }

            // 転送制御機能が無効の場合、以降のプロパティは無視する
            if(IsTransCtrlEnabled)
            {
                // 帯域制御機能を有効化するかどうか
                bool isBandCtrlEnabled = false;
                try
                {
                    isBandCtrlEnabled = Util.GetRequiredValue<bool>(desiredProperties, "bandwidthcontrol");
                    MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property bandwidthcontrol is: {isBandCtrlEnabled}");
                }
                catch (Exception ex)
                {
                    var errmsg = $"Property bandwidthcontrol dose not exist.";
                    MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"{errmsg} {ex}", true);
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Exit Method: OnDesiredPropertiesReceivedAsync caused by {errmsg}");
                    retStatus = false;
                    return retStatus;
                }

                // メッセージバッファのサイズ
                int sendSizeMax = 0;
                try
                {
                    sendSizeMax = Util.GetRequiredValue<int>(desiredProperties, "sendsizemax");
                    MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property sendsizemax is: {sendSizeMax}");
                }
                catch (Exception ex)
                {
                    var errmsg = $"Property sendsizemax dose not exist.";
                    MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"{errmsg} {ex}", true);
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Exit Method: OnDesiredPropertiesReceivedAsync caused by {errmsg}");
                    retStatus = false;
                    return retStatus;
                }

                // メッセージ送信サイクル
                int sendCycle = 0;
                try
                {
                    sendCycle = Util.GetRequiredValue<int>(desiredProperties, "sendcycle");
                    MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property sendcycle is: {sendCycle}");
                }
                catch (Exception ex)
                {
                    var errmsg = $"Property sendcycle dose not exist.";
                    MyLogger.WriteLog(ILogger.LogLevel.ERROR, $"{errmsg} {ex}", true);
                    MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"Exit Method: OnDesiredPropertiesReceivedAsync caused by {errmsg}");
                    retStatus = false;
                    return retStatus;
                }

                // バッファ条件
                string[] unitKeys = null;
                try
                {
                    string strUnitKeys = Util.GetRequiredValue<string>(desiredProperties, "unitkey");
                    unitKeys = strUnitKeys.Split(",");

                    foreach (string s in unitKeys)
                    {
                        MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property unitkey is: {s}");
                    }
                }
                catch (Exception)
                {
                    MyLogger.WriteLog(ILogger.LogLevel.INFO, $"Property unitkey dose not exist and set single unit mode.");
                }

                // TransportControllerのインスタンスを作成
                TransportController.SetSettings(isBandCtrlEnabled, sendSizeMax, sendCycle, unitKeys);
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
                retStatus = false;
            }
            // ＝＝＝＝＝＝＝＝＝＝＝＝＝ここまで＝＝＝＝＝＝＝＝＝＝＝＝＝

            MyLogger.WriteLog(ILogger.LogLevel.DEBUG, $"Return status : {retStatus}");
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, $"End Method: OnMessageReceivedAsync");
            return retStatus;
        }

        /// OnMessageReceivedAsync/Flushから呼び出しのみ、Set/Unset不要
        public static async Task<bool> SendMessage(byte[] body, IDictionary<string, string> properties)
        {
            MyLogger.WriteLog(ILogger.LogLevel.TRACE, "Start Method: SendMessage");

            bool retStatus = true;
            IApplicationEngine engine = ApplicationEngineFactory.GetEngine();

            try
            {
                // メッセージをエンコードして送信
                await engine.SendMessageAsync(OutputName, MyEncoder.EncodeMessage(body, properties));
                retStatus = true;
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