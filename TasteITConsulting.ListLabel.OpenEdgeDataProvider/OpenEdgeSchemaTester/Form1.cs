/**********************************************************************
 * Copyright (C) 2017 by Taste IT Consulting ("TIC") -                *
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
using System.Text;
using System.Windows.Forms;
using combit.Reporting;
using TasteITConsulting.Reporting;
using System.IO;
using System.Collections.ObjectModel;
using combit.Reporting.DataProviders;

namespace OpenEdgeSchemaTester
{
    public partial class Form1 : Form
    {
        private string _schema = "";

        public Form1()
        {
            InitializeComponent();
        }

        private void buttonFile_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();

            //openFileDialog1.InitialDirectory = "c:\\";
            openFileDialog1.Filter = "Json files (*.json)|*.json|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 2;
            //openFileDialog1.RestoreDirectory = true;

            if (openFileDialog1.ShowDialog() == DialogResult.OK)
            {
                textFilename.Text = openFileDialog1.FileName;
                try
                {
                    _schema = File.ReadAllText(openFileDialog1.FileName, Encoding.UTF8);
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Error: Could not read file from disk. Original error: " + ex.Message);
                }

                OpenEdgeServiceCatalogReader reader = new OpenEdgeServiceCatalogReader();
                TextServiceName.Text = reader.ReadSchema(_schema);

                OpenEdgeDataProvider dp = new OpenEdgeDataProvider();
                ServiceAdapter s = new ServiceAdapter(_schema);
                dp.ServiceName = TextServiceName.Text;
                dp.ServiceAdapter = s;
                dp.Initialize();

                ReadOnlyCollection <ITable> tables;
                tables = dp.Tables;

                TableCombo.Items.Clear();
                TableCombo.Items.Add("<none>");
                foreach (ITable t in tables)
                {
                    TableCombo.Items.Add(t.TableName);
                }
                TableCombo.SelectedIndex = 0;
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            ListLabel LL = new ListLabel();
            OpenEdgeDataProvider dp = new OpenEdgeDataProvider();
            ServiceAdapter s = new ServiceAdapter(_schema);

            try
            {
                dp.ServiceName = TextServiceName.Text;
                dp.ServiceAdapter = s;
                dp.Initialize();

                if (TableCombo.Text != "<none>")
                {
                    LL.DataMember = TableCombo.Text;
                    if (AutoMasterModeNone.Checked)
                        LL.AutoMasterMode = LlAutoMasterMode.None;
                    if (AutomasterModeAsVariables.Checked)
                        LL.AutoMasterMode = LlAutoMasterMode.AsVariables;
                    if (AutoMasterModeAsFields.Checked)
                        LL.AutoMasterMode = LlAutoMasterMode.AsFields;
                }

                LL.DataSource = dp;
                LL.Design();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error: " + ex.Message);
            }
            LL.Dispose();
        }
    }

    internal class ServiceAdapter : IServiceAdapter
    {
        private string _schema = "";

        public ServiceAdapter ( string schema)
        {
            _schema = schema;
        }

        public bool GetData(string ServiceName, OELongchar JsonServiceParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse)
        {
            throw new NotImplementedException();
        }

        public bool GetSchema(string ServiceName, OELongchar JsonServiceParameter, out OELongchar JsonSchema)
        {
            JsonSchema = new OELongchar();
            JsonSchema.Data = _schema;
            return true;
        }

        public bool ClientEvent(string ServiceName, OELongchar JsonParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse)
        {
            JsonDataResponse = new OELongchar();
            return true;
        }

    }

    // Catalog reader for Service definitions - like the one in the dataprovider.
    // We just need the service name.
    internal class OpenEdgeServiceCatalogReader
    {
        JsonData Catalog;
        JsonData CatalogData;

        public string ReadSchema(string jsonString)
        {
            string name = "";
            CatalogData = JsonMapper.ToObject(jsonString);
            Catalog = CatalogData["OpenEdgeServiceCatalog"];
            name = ReadService(Catalog);
            return name;
        }

        private string ReadService(JsonData Catalog)
        {
            JsonData Service;
            string name = "";
            for (int i = 0; i < Catalog["OpenEdgeService"].Count; i++)
            {
                Service = Catalog["OpenEdgeService"][i];
                name = Service["ServiceName"].ToString();
            }
            return name;
        }
    }
}
