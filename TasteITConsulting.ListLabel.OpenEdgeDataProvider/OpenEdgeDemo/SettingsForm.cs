using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace OpenEdgeDemo
{
    internal partial class SettingsForm : Form
    {
        public OpenEdgeConfiguration Settings  { get; set;}
            
        public SettingsForm()
        {
            InitializeComponent();
        }

        private void Settings_Load(object sender, EventArgs e)
        {
            SettingsGrid.SelectedObject = Settings;
        }

        private void buttonOK_Click(object sender, EventArgs e)
        {
            this.DialogResult = DialogResult.OK;
            Close();
        }

        private void SettingsGrid_Click(object sender, EventArgs e)
        {

        }
    }
}
