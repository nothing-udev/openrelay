using System;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;
using UnityEngine.Networking;

namespace OpenRelay.Api
{
    [Serializable]
    public class SessionInfo
    {
        public string joinCode;
        public string wsEndpoint;  // e.g. "ws://my-server:7777/relay"
        public string udpEndpoint; // e.g. "my-server:7779"
        public int    peerCount;
        public string token;       // HMAC-SHA256 auth token; "" = auth disabled
    }

    //  Thin HTTP client for the OpenRelay session management API.
    public static class OpenRelayApiClient
    {
        //  Asks the server to create a new session.
        public static Task<SessionInfo> CreateSessionAsync(string apiBaseUrl)
            => SendAsync<SessionInfo>(
                UnityWebRequest.kHttpVerbPUT,
                apiBaseUrl.TrimEnd('/') + "/api/v1/sessions/create",
                "{}");

        //  Looks up an existing session by join code.
        public static Task<SessionInfo> JoinSessionAsync(string apiBaseUrl, string joinCode)
            => SendAsync<SessionInfo>(
                UnityWebRequest.kHttpVerbPOST,
                apiBaseUrl.TrimEnd('/') + "/api/v1/sessions/join",
                $"{{\"joinCode\":\"{joinCode}\"}}");


        private static Task<T> SendAsync<T>(string method, string url, string body)
        {
            var tcs = new TaskCompletionSource<T>();
            var req = new UnityWebRequest(url, method)
            {
                uploadHandler   = new UploadHandlerRaw(Encoding.UTF8.GetBytes(body)),
                downloadHandler = new DownloadHandlerBuffer(),
            };
            req.SetRequestHeader("Content-Type", "application/json");

            req.SendWebRequest().completed += _ =>
            {
                try
                {
                    if (req.result != UnityWebRequest.Result.Success)
                    {
                        tcs.SetException(new Exception(
                            $"[OpenRelay] HTTP {req.responseCode}: {req.error}\n{req.downloadHandler.text}"));
                        return;
                    }
                    tcs.SetResult(JsonUtility.FromJson<T>(req.downloadHandler.text));
                }
                catch (Exception ex) { tcs.SetException(ex); }
                finally  { req.Dispose(); }
            };
            return tcs.Task;
        }
    }
}
