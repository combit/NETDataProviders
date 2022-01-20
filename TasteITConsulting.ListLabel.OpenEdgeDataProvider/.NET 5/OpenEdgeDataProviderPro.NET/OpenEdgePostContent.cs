/**********************************************************************
 * Copyright (C) 2021 by Taste IT Consulting ("TIC") -                *
 * www.taste-consulting.de and other contributors as listed           *
 * below.  All Rights Reserved.                                       *
 *                                                                    *
 *  Software is distributed on an "AS IS", WITHOUT WARRANTY OF ANY    *
 *  KIND, either express or implied.                                  *
 *  See the Microsoft Public License (Ms-PL) for more details.        *
 *  You should have received a copy of the Microsoft Public License   *
 *  in <license.txt> along with this software. If not, see            *
 *  <http://www.microsoft.com/en-us/openness/licenses.aspx#MPL>.      *
 *                                                                    *
 *  Contributors:                                                     *
 *                                                                    *
 **********************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasteITConsulting.Reporting
{

    public class OpenEdgePostContent
    {
        private List<OEPostContent> _Post;

        public OpenEdgePostContent()
        {
            _Post = new List<OEPostContent>();
        }

        public void AddContent(string contentName, string contentData)
        {
            _Post.Add(new OEPostContent(contentName, contentData));
        }


        public string GetJson()
        {
            StringBuilder sb = new StringBuilder();
            JsonWriter w = new JsonWriter(sb);
            w.PrettyPrint = true;
            try
            {
                // Dataset
                w.WriteObjectStart();
                w.WritePropertyName("OpenEdgePostContent");
                w.WriteObjectStart();

                if (_Post.Count > 0)
                {
                    // TempTable 
                    w.WritePropertyName("OEPostContent");
                    w.WriteArrayStart();

                    // TempTable Row 
                    foreach (OEPostContent _content in _Post)
                    {
                        w.WriteObjectStart();
                        w.WritePropertyName("OEContentName");
                        w.Write(_content.OEContentName);
                        w.WritePropertyName("OEContentData");
                        w.Write(_content.OEContentData);
                        w.WriteObjectEnd();
                    }
                    // TempTable
                    w.WriteArrayEnd();
                }
                // Dataset
                w.WriteObjectEnd();
                w.WriteObjectEnd();
            }
            catch (JsonException)
            {
                return null;
            }
            return sb.ToString();
        }
        internal class OEPostContent
        {
            public string OEContentName { get; set; }
            public string OEContentData { get; set; }
            public OEPostContent(string contentName, string contentData)
            {
                OEContentName = contentName;
                OEContentData = contentData;
            }
        }
    }
}
