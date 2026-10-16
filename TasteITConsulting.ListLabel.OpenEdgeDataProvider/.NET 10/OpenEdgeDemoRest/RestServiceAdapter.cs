using OpenEdgeDemo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using TasteITConsulting.Reporting;

namespace OpenEdgeDemo
{
    class RestServiceAdapter : IServiceAdapter
    {
        private RestConnectionParameter _RestConnectionParameter;
        public RestServiceAdapter(RestConnectionParameter restConnectionParameter)
        {
            _RestConnectionParameter = restConnectionParameter;
        }
        
        public bool ClientEvent(string ServiceName, OELongchar JsonServiceParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse)
        {
            JsonDataResponse = new OELongchar();
            var post = new OpenEdgePostContent();
            post.AddContent("MethodName", "ClientEvent");
            post.AddContent("ServiceName", ServiceName);
            post.AddContent("OpenEdgeServiceParameter", JsonServiceParameter.Data);
            post.AddContent("OpenEdgeDataRequest", JsonDataRequest.Data);
            var response = HttpRequest(post.GetJson());
            JsonDataResponse.Data = response;
            return true;
        }

        public bool GetData(string ServiceName, OELongchar JsonServiceParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse)
        {
            JsonDataResponse = new OELongchar();
            var post = new OpenEdgePostContent();
            post.AddContent("MethodName", "GetData");
            post.AddContent("ServiceName", ServiceName);
            post.AddContent("OpenEdgeServiceParameter", JsonServiceParameter.Data);
            post.AddContent("OpenEdgeDataRequest", JsonDataRequest.Data);
            var response = HttpRequest(post.GetJson());
            JsonDataResponse.Data = response;
            return true;
        }

        public bool GetSchema(string ServiceName, OELongchar JsonServiceParameter, out OELongchar JsonSchema)
        {
            JsonSchema = new OELongchar();
            var post = new OpenEdgePostContent();
            post.AddContent("MethodName", "GetSchema");
            post.AddContent("ServiceName", ServiceName);
            post.AddContent("OpenEdgeServiceParameter", JsonServiceParameter.Data);
            var response = HttpRequest(post.GetJson());
            JsonSchema.Data = response;
            return true;
        }

        private string HttpRequest(string postContent)
        {
            var httpClient = new HttpClient();
            var resturl = _RestConnectionParameter.RestURL;
            var data = new StringContent(postContent, Encoding.UTF8, "application/json");
            var myTask = httpClient.PostAsync(resturl, data);
            var myString = myTask.GetAwaiter().GetResult().Content.ReadAsStringAsync();
            return myString.Result.ToString();
        }
    }

    public class RestConnectionParameter
    {
        public string RestURL { get; set; }
        public string Userid  { get; set; }
        public string Password { get; set; }
    }

}

