using combit.Reporting;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using TasteITConsulting.Reporting;

namespace OpenEdgeDemo
{
    public partial class DemoForm : Form
    {
        OpenEdgeConfiguration Config = null;

        public DemoForm()
        {
            InitializeComponent();
            Config = new OpenEdgeConfiguration();
            Config.loadSettings();
        }

        private void LLAction (string action)
        {
            var param = new RestConnectionParameter();
            param.RestURL  = Properties.Settings.Default.RestURL;
            param.Userid   = Properties.Settings.Default.Userid;
            param.Password = Properties.Settings.Default.Password;

            var ll = new ListLabel();
            var dp = new OpenEdgeDataProvider();
            dp.ServiceAdapter = new RestServiceAdapter(param);
            dp.ServiceName = Properties.Settings.Default.ServiceName;
            dp.Initialize();
            ll.DataSource = dp;
            try
            {
                if (action == "DESIGN")
                {
                    if (Properties.Settings.Default.DesignerPreviewMaxRows > 0)
                    {
                        dp.MaxRows = Properties.Settings.Default.DesignerPreviewMaxRows;
                    }
                    ll.Design();
                }
                else if (action == "PRINT")
                {
                    ll.Print();
                }
                else
                {

                }
            }
            catch (ListLabelException e)
            {
                MessageBox.Show(e.Message);
            }
            ll.Dispose();
            
            dp.Dispose();

        }

        private void buttonDesign_Click(object sender, EventArgs e)
        {
            LLAction("DESIGN");
        }

        private void buttonPrint_Click(object sender, EventArgs e)
        {
            LLAction("PRINT");
            
        }

        private void buttonSettings_Click(object sender, EventArgs e)
        {
            SettingsForm f = new SettingsForm();
            f.Settings = Config;
            if (f.ShowDialog(this) == DialogResult.OK)
            {
                Config.saveSettings();
                /*
                if (OpenEdgeServiceAdapter != null)
                {
                    OpenEdgeServiceAdapter.Disconnect();
                }
                */
            }
            f.Dispose();
        }
    }

    public class OpenEdgeConfiguration
    {
        public string RestURL { get; set; }
        public string Userid { get; set; }
        public string Password { get; set; }
        public string ServiceName { get; set; }
        public int DesignerPreviewMaxRows { get; set; }

        public void loadSettings()
        {
            this.ServiceName = Properties.Settings.Default.ServiceName;
            this.RestURL = Properties.Settings.Default.RestURL;
            this.Userid = Properties.Settings.Default.Userid;
            this.Password = Properties.Settings.Default.Password;
            this.DesignerPreviewMaxRows = Properties.Settings.Default.DesignerPreviewMaxRows;
        }

        public void saveSettings()
        {
            Properties.Settings.Default.ServiceName = this.ServiceName;
            Properties.Settings.Default.RestURL = this.RestURL;
            Properties.Settings.Default.Userid = this.Userid;
            Properties.Settings.Default.Password = this.Password;
            Properties.Settings.Default.DesignerPreviewMaxRows = this.DesignerPreviewMaxRows;
            Properties.Settings.Default.Save();
        }

    }


}
