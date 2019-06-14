/**********************************************************************
 * Copyright (C) 2014 by Taste IT Consulting ("TIC") -                *
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
using System.Windows.Forms;
using combit.ListLabel24;
using TasteITConsulting.ListLabel24;
using TasteITConsulting.ListLabel;

namespace OpenEdgeDemo
{
    public partial class OpenEdgeDemoForm : Form
    {
        ServiceAdapter OpenEdgeServiceAdapter = null;
        OpenEdgeConfiguration Config = null;
        public OpenEdgeDemoForm()
        {
            InitializeComponent();
            Config = new OpenEdgeConfiguration();
            Config.loadSettings();
            displayConfig();
        }

        private void displayConfig()
        {
            /*
            ServiceName.Text = Config.ServiceName;
            AppServerURL.Text = Config.AppServerURL;
            Userid.Text = Config.Userid;
            Password.Text = Config.Password;
            AppServerInfo.Text = Config.AppServerInfo;
            */
        }

        private bool ensureService()
        {
            if (OpenEdgeServiceAdapter == null)
            {
                OpenEdgeServiceAdapter = new ServiceAdapter();
            }
            if ( !OpenEdgeServiceAdapter.Connected)
            {
                OpenEdgeServiceAdapter.Connect(Config);
            }
            return OpenEdgeServiceAdapter.Connected;
        }

        private void LLAction (string action)
        {
            if (ensureService())
            {
                ListLabel LL = new ListLabel();
                OpenEdgeDataProvider Provider = new OpenEdgeDataProvider();

                Provider.ServiceAdapter = OpenEdgeServiceAdapter;
                Provider.ServiceName    = Config.ServiceName;
                Provider.Initialize();
                LL.DataSource = Provider;

                try
                {
                    if (action == "Design")
                    {
                        if (Config.DesignerPreviewMaxRows > 0)
                            Provider.MaxRows = Config.DesignerPreviewMaxRows;
                       LL.Design();
                    }
                    else
                        LL.Print();
                }
                catch (ListLabelException ex)
                {
                    MessageBox.Show(ex.Message);
                }
                finally
                {
                    Provider.Dispose();
                    LL.Dispose();
                }
            }
            else
            {
                MessageBox.Show("No connection available");
            }
        }

        private void btnDesign_Click(object sender, EventArgs e)
        {
            LLAction("Design");
        }

        private void btnPrint_Click(object sender, EventArgs e)
        {
            LLAction("Print");
        }

        private void btnPing_Click(object sender, EventArgs e)
        {
            if (ensureService())
            {
                bool Success = OpenEdgeServiceAdapter.Ping();
                if (Success)
                {
                    MessageBox.Show("Appserver is connected and the gateway was found :-)");
                }
                else
                {
                    MessageBox.Show("Appserver is connected but the gateway was not found :-(");
                }
            }
            else
            {
                MessageBox.Show("Appserver is not connected :-(");
            }
        }

        private void Sport2000SampleForm_Load(object sender, EventArgs e)
        {

        }

        private void btnLoad_Click(object sender, EventArgs e)
        {

        }

        private void btnSave_Click(object sender, EventArgs e)
        {
            Config.saveSettings();
            if (OpenEdgeServiceAdapter != null)
            {
                OpenEdgeServiceAdapter.Disconnect();
            }
        }

        private void btnSettings_Click(object sender, EventArgs e)
        {
           SettingsForm f = new SettingsForm();
            f.Settings = Config;
           if (f.ShowDialog(this) == DialogResult.OK)
           {
                Config.saveSettings();
                if (OpenEdgeServiceAdapter != null)
                {
                    OpenEdgeServiceAdapter.Disconnect();
                }
            }
            f.Dispose();
        }
    }

    internal class OpenEdgeConfiguration
    {
        public string ServiceName { get; set; } 
        public string AppServerURL { get; set; }
        public string Userid { get; set; }
        public string Password { get; set; }
        public string AppServerInfo { get; set; }
        public string LayoutPath { get; set; }
        public int    DesignerPreviewMaxRows { get; set; }

        public void loadSettings()
        { 
            this.ServiceName   = Properties.Settings.Default.ServiceName;
            this.AppServerURL  = Properties.Settings.Default.AppServerURL;
            this.AppServerInfo = Properties.Settings.Default.AppServerInfo;
            this.Userid        = Properties.Settings.Default.Userid;
            this.Password      = Properties.Settings.Default.Password;
            this.LayoutPath    = Properties.Settings.Default.LayoutPath;
            this.DesignerPreviewMaxRows = Properties.Settings.Default.DesignerPreviewMaxRows;
        }

        public void saveSettings()
        {
            Properties.Settings.Default.ServiceName   = this.ServiceName;
            Properties.Settings.Default.AppServerURL  = this.AppServerURL;
            Properties.Settings.Default.AppServerInfo = this.AppServerInfo;
            Properties.Settings.Default.Userid        = this.Userid;
            Properties.Settings.Default.Password      = this.Password;
            Properties.Settings.Default.LayoutPath    = this.LayoutPath;
            Properties.Settings.Default.DesignerPreviewMaxRows = this.DesignerPreviewMaxRows;
            Properties.Settings.Default.Save();
        } 

    }

    internal class ServiceAdapter : IServiceAdapter
    {
        OpenEdgeProxy _Proxy;
        OpenEdgeConfiguration _config;

        //string AppServerURL = "AppServer://localhost:5162/listlabel";
        public bool Connected {get {return _Proxy != null;}}

        public ServiceAdapter()
        {
        }

        public bool Connect (OpenEdgeConfiguration Configuration)
        {
            _config = Configuration;
            try
            {
                _Proxy = new OpenEdgeProxy(_config.AppServerURL, _config.Userid, _config.Password, _config.AppServerInfo);
            }
            catch (Progress.Open4GL.Exceptions.ConnectException ex)
            {
                MessageBox.Show(ex.ToString());
                return false;
            }
            return true;
        }

        public void Disconnect ()
        {
            if (_Proxy != null)
                _Proxy.Dispose();
        }

        public bool GetData(string ServiceName, OELongchar JsonParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse)
        {
            JsonDataResponse = new OELongchar();
            string response;
            _Proxy.OpenEdgeGateway(ServiceName, "GetData", JsonParameter.Data, JsonDataRequest.Data, out response);
            JsonDataResponse.Data = response;
            return true;
        }

        public bool GetSchema(string ServiceName, OELongchar JsonParameter, out OELongchar JsonSchema) 
        {
            string response;
            JsonSchema = new OELongchar();
            _Proxy.OpenEdgeGateway(ServiceName, "GetSchema", JsonParameter.Data , "", out response);
            JsonSchema.Data = response;
            return true;
        }

        public bool ClientEvent(string ServiceName, OELongchar JsonParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse)
        {
            string response;
            _Proxy.OpenEdgeGateway(ServiceName, "ClientEvent", JsonParameter.Data, JsonDataRequest.Data, out response);
            JsonDataResponse = new OELongchar();
            JsonDataResponse.Data = response;
            return true;
        }

        public bool Ping()
        {
            string response = null;
            try
            {
                _Proxy.OpenEdgeGateway("", "Ping","", "", out response);
            }
            catch (Progress.Open4GL.Exceptions.ConnectException ex)
            {
                MessageBox.Show(ex.ToString());
            }
            if (response == null)
                return false;
            if (response == "Pong")
                return true;

            return false; 
        }

    }


}
