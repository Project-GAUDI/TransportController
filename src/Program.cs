namespace TransportController
{
    using System;
    using System.Runtime.Loader;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Shared;
    using TICO.GAUDI.Commons;

    class Program
    {
        static IModuleClient MyModuleClient { get; set; } = null;

        static Logger MyLogger { get; } = Logger.GetLogger(typeof(Program));

        static string InputName { get; set; } = string.Empty;

        static string OutputName { get; set; } = string.Empty;

        static bool IsTransCtrlEnabled { get; set; }

        static MessageEncoder MyEncoder { get; set; }

        static void Main(string[] args)
        {
            try
            {
                Init().Wait();
            }
            catch (Exception e)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"Init failed. {e}", true);
                Environment.Exit(1);
            }

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();

            // 終了前に累積したメッセージを送信する
            IsTransCtrlEnabled = false;
            TransportController.FlushAndDisposeAll().Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Start Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);

            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// </summary>
        static async Task Init()
        {
            if (MyModuleClient != null)
            {
                // 累積したメッセージを送信してインスタンスを開放する
                IsTransCtrlEnabled = false;
                await TransportController.FlushAndDisposeAll();

                // 取得済みのModuleClientを解放する
                await MyModuleClient.CloseAsync();
                MyModuleClient.Dispose();
                MyModuleClient = null;
            }

            // 環境変数から送信トピックを判定
            TransportTopic defaultSendTopic = TransportTopic.Iothub;
            string sendTopicEnv = Environment.GetEnvironmentVariable("DefaultSendTopic");
            if (Enum.TryParse(sendTopicEnv, true, out TransportTopic sendTopic))
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Evironment Variable \"DefaultSendTopic\" is {sendTopicEnv}.");
                defaultSendTopic = sendTopic;
            }
            else
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Evironment Variable \"DefaultSendTopic\" is not set. Default value ({defaultSendTopic}) assigned.");
            }

            // 環境変数から受信トピックを判定
            TransportTopic defaultReceiveTopic = TransportTopic.Iothub;
            string receiveTopicEnv = Environment.GetEnvironmentVariable("DefaultReceiveTopic");
            if (Enum.TryParse(receiveTopicEnv, true, out TransportTopic receiveTopic))
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Evironment Variable \"DefaultReceiveTopic\" is {receiveTopicEnv}.");
                defaultReceiveTopic = receiveTopic;
            }
            else
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Evironment Variable \"DefaultReceiveTopic\" is not set. Default value ({defaultReceiveTopic}) assigned.");
            }

            // MqttModuleClientを作成
            if (Boolean.TryParse(Environment.GetEnvironmentVariable("M2MqttFlag"), out bool m2mqttFlag) && m2mqttFlag)
            {
                string sasTokenEnv = Environment.GetEnvironmentVariable("SasToken");
                MyModuleClient = new MqttModuleClient(sasTokenEnv, defaultSendTopic: defaultSendTopic, defaultReceiveTopic: defaultReceiveTopic);
            }
            // IoTHubModuleClientを作成
            else
            {
                ITransportSettings[] settings = null;
                string protocolEnv = Environment.GetEnvironmentVariable("TransportProtocol");
                if (Enum.TryParse(protocolEnv, true, out TransportProtocol transportProtocol))
                {
                    MyLogger.WriteLog(Logger.LogLevel.INFO, $"Evironment Variable \"TransportProtocol\" is {protocolEnv}.");
                    settings = transportProtocol.GetTransportSettings();
                }
                else
                {
                    MyLogger.WriteLog(Logger.LogLevel.INFO, $"Evironment Variable \"TransportProtocol\" is not set. Default value ({TransportProtocol.Amqp}) assigned.");
                }

                MyModuleClient = await IotHubModuleClient.CreateAsync(settings, defaultSendTopic, defaultReceiveTopic).ConfigureAwait(false);
            }

            // edgeHubへの接続
            while (true)
            {
                try
                {
                    await MyModuleClient.OpenAsync().ConfigureAwait(false);
                    break;
                }
                catch (Exception e)
                {
                    MyLogger.WriteLog(Logger.LogLevel.WARN, $"Open a connection to the Edge runtime is failed. {e.Message}");
                    await Task.Delay(1000);
                }
            }

            // Loggerへモジュールクライアントを設定
            Logger.SetModuleClient(MyModuleClient);

            // 環境変数からログレベルを設定
            string logEnv = Environment.GetEnvironmentVariable("LogLevel");
            try
            {
                if (logEnv != null) Logger.SetOutputLogLevel(logEnv);
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Output log level is: {Logger.OutputLogLevel.ToString()}");
            }
            catch (ArgumentException)
            {
                MyLogger.WriteLog(Logger.LogLevel.WARN, $"Environment LogLevel does not expected string. Default value ({Logger.OutputLogLevel.ToString()}) assigned.");
            }

            // desiredプロパティの取得
            var twin = await MyModuleClient.GetTwinAsync().ConfigureAwait(false);
            var collection = twin.Properties.Desired;
            bool isready = false;
            try
            {
                SetMyProperties(collection);
                isready = true;
            }
            catch (Exception e)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"SetMyProperties failed. {e}", true);
                isready = false;
            }

            // プロパティ更新時のコールバックを登録
            await MyModuleClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertiesUpdate, null).ConfigureAwait(false);

            if (isready)
            {
                // Register callback to be called when a message is received by the module
                await MyModuleClient.SetInputMessageHandlerAsync(InputName, ReceiveMessage, null).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// プロパティ更新時のコールバック処理
        /// </summary>
        static async Task OnDesiredPropertiesUpdate(TwinCollection desiredProperties, object userContext)
        {
            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Start Method: OnDesiredPropertiesUpdate");
            MyLogger.WriteLog(Logger.LogLevel.INFO, "Updating desired properties.");

            try
            {
                await Init();
            }
            catch (Exception e)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"OnDesiredPropertiesUpdate failed. {e}", true);
            }

            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: OnDesiredPropertiesUpdate");
        }


        /// <summary>
        /// メッセージ受信時のコールバック処理
        /// </summary>
        static async Task<MessageResponse> ReceiveMessage(IotMessage message, object userContext)
        {
            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Start Method: ReceiveMessage");
            try
            {
                byte[] messageBytes = message.GetBytes();
                IDictionary<string, string> properties = message.GetProperties();

                MyLogger.WriteLog(Logger.LogLevel.INFO, "1 message received.");

                if ((int)Logger.OutputLogLevel <= (int)Logger.LogLevel.TRACE)
                {
                    string messageString = Encoding.UTF8.GetString(messageBytes);
                    MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Received Message. Body: [{messageString}]");
                }

                if (IsTransCtrlEnabled)
                {
                    // メッセージをバッファに貯める
                    var input = JsonMessage.DeserializeJsonMessage(messageBytes);
                    MyLogger.WriteLog(Logger.LogLevel.INFO, $"Buffering {input.RecordList.Count} records.");
                    foreach (var record in input.RecordList)
                    {
                        await TransportController.SaveMessage(JsonMessage.SerializeRecordInfoByte(record), properties);
                    }
                }
                else
                {
                    // メッセージを送信
                    MyLogger.WriteLog(Logger.LogLevel.INFO, $"Send 1 message.");
                    await SendMessage(messageBytes, properties);
                }
            }
            catch (Exception e)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"ReceiveMessage failed. {e}", true);
            }

            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: ReceiveMessage");
            return MessageResponse.Completed;
        }

        public static async Task SendMessage(byte[] body, IDictionary<string, string> properties)
        {
            MyLogger.WriteLog(Logger.LogLevel.TRACE, "Start Method: SendMessage");

            // メッセージをエンコードして送信
            await MyModuleClient.SendEventAsync(OutputName, MyEncoder.EncodeMessage(body, properties));
            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: SendMessage");
        }


        /// <summary>
        /// desiredプロパティから自クラスのプロパティをセットする
        /// </summary>
        static void SetMyProperties(TwinCollection desiredProperties)
        {
            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Start Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            // MessageInput
            InputName = "input";
            try
            {
                InputName = desiredProperties["input"];
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property input is: {InputName}");
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property input dose not exist and set \"{InputName}\"");
            }

            // MessageOutput
            OutputName = "output";
            try
            {
                OutputName = desiredProperties["output"];
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property output is: {OutputName}");
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property output dose not exist and set \"{OutputName}\"");
            }

            // 圧縮モード
            // Compress
            string compress = "none";
            try
            {
                compress = desiredProperties["compress"];
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property compress is: {compress}");
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property compress dose not exist and set \"{compress}\"");
            }

            // MessageEncoderのインスタンス作成
            MyEncoder = new MessageEncoder(compress);

            // 転送制御機能を有効化するかどうか
            try
            {
                IsTransCtrlEnabled = desiredProperties["transportcontrol"];
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property transportcontrol is: {IsTransCtrlEnabled}");
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"Property transportcontrol dose not exist.", true);
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Exit Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
                throw;
            }

            // 転送制御機能が無効の場合、以降のプロパティは無視する
            if(!IsTransCtrlEnabled)
            {
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
                return;
            }

            // 帯域制御機能を有効化するかどうか
            bool isBandCtrlEnabled = false;
            try
            {
                isBandCtrlEnabled = desiredProperties["bandwidthcontrol"];
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property bandwidthcontrol is: {isBandCtrlEnabled}");
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"Property bandwidthcontrol dose not exist.", true);
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Exit Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
                throw;
            }

            // メッセージバッファのサイズ
            int sendSizeMax = 0;
            try
            {
                sendSizeMax = desiredProperties["sendsizemax"];
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property sendsizemax is: {sendSizeMax}");
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"Property sendsizemax dose not exist.", true);
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Exit Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
                throw;
            }

            // メッセージ送信サイクル
            int sendCycle = 0;
            try
            {
                sendCycle = desiredProperties["sendcycle"];
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property sendcycle is: {sendCycle}");
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.ERROR, $"Property sendcycle dose not exist.", true);
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Exit Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
                throw;
            }

            // バッファ条件
            string[] unitKeys = null;
            try
            {
                string strUnitKeys = desiredProperties["unitkey"].ToString();
                unitKeys = strUnitKeys.Split(",");

                foreach (string s in unitKeys)
                {
                    MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property unitkey is: {s}");
                }
            }
            catch (ArgumentOutOfRangeException)
            {
                MyLogger.WriteLog(Logger.LogLevel.INFO, $"Property unitkey dose not exist and set single unit mode.");
            }

            // TransportControllerのインスタンスを作成
            TransportController.SetSettings(isBandCtrlEnabled, sendSizeMax, sendCycle, unitKeys);
            MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");

        }
    }
}
