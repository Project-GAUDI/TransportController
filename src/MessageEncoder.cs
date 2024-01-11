namespace TransportController
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.IO.Compression;
    using Microsoft.Azure.Devices.Client;
    using TICO.GAUDI.Commons;

    /// <summary>
    /// メッセージエンコードクラス
    /// </summary>
    class MessageEncoder
    {
        public enum Compress
        {
            GZIP,
            DEFLATE,
            NONE
        }

        public Compress CompType { get; private set; }

        private Logger MyLogger { get; set; }

        public MessageEncoder(string compress)
        {
            MyLogger = Logger.GetLogger(this.GetType());

            string tmp = compress.ToLower();
            switch (tmp)
            {
                case "gzip":
                    CompType = Compress.GZIP;
                    break;
                case "deflate":
                    CompType = Compress.DEFLATE;
                    break;
                default:
                    CompType = Compress.NONE;
                    break;
            }
        }

        /// <summary>
        /// メッセージの圧縮とID、プロパティの付与を行う
        /// </summary>
        /// <param name="body">本文</param>
        /// <param name="properties">プロパティ</param>
        /// <returns>メッセージ</returns>
        public IotMessage EncodeMessage(byte[] body, IDictionary<string, string> properties)
        {
            if ((int)Logger.OutputLogLevel <= (int)Logger.LogLevel.TRACE)
            {
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Start Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            }

            // bodyの圧縮
            byte[] compBody;
            switch (CompType)
            {
                case Compress.GZIP:
                    compBody = GzipBytes(body);
                    break;
                case Compress.DEFLATE:
                    compBody = DeflateBytes(body);
                    break;
                case Compress.NONE:
                default:
                    compBody = body;
                    break;
            }

            var msg = new IotMessage(compBody);
            msg.message.MessageId = Util.GetMessageId();

            msg.SetProperties(properties);

            msg.SetProperty("iotedge_timestamp", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") );

            if ((int)Logger.OutputLogLevel <= (int)Logger.LogLevel.TRACE)
            {
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            }

            return msg;
        }

        /// <summary>
        /// Gzip圧縮処理
        /// </summary>
        /// <param name="msg">メッセージ本文</param>
        /// <returns></returns>
        private byte[] GzipBytes(byte[] msg)
        {
            if ((int)Logger.OutputLogLevel <= (int)Logger.LogLevel.TRACE)
            {
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Start Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            }
            
            byte[] ret;

            using (MemoryStream instream = new MemoryStream(msg))
            using (MemoryStream outstream = new MemoryStream())
            {
                using (var gs = new GZipStream(outstream, CompressionMode.Compress))
                {
                    instream.CopyTo(gs);
                }
                ret = outstream.ToArray();
            }

            if ((int)Logger.OutputLogLevel <= (int)Logger.LogLevel.TRACE)
            {
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            }

            return ret;
        }

        /// <summary>
        /// Deflate圧縮処理
        /// </summary>
        /// <param name="msg">メッセージ本文</param>
        /// <returns></returns>
        private byte[] DeflateBytes(byte[] msg)
        {
            if ((int)Logger.OutputLogLevel <= (int)Logger.LogLevel.TRACE)
            {
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"Start Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            }
            
            byte[] ret;

            using (MemoryStream instream = new MemoryStream(msg))
            using (MemoryStream outstream = new MemoryStream())
            {
                using (var ds = new DeflateStream(outstream, CompressionMode.Compress))
                {
                    instream.CopyTo(ds);
                }
                ret = outstream.ToArray();
            }

            if ((int)Logger.OutputLogLevel <= (int)Logger.LogLevel.TRACE)
            {
                MyLogger.WriteLog(Logger.LogLevel.TRACE, $"End Method: {System.Reflection.MethodBase.GetCurrentMethod().Name}");
            }

            return ret;
        }
    }
}
