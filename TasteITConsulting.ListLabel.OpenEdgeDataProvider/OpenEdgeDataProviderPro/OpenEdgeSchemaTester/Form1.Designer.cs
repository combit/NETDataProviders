namespace OpenEdgeSchemaTester
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.TextServiceName = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.textFilename = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.buttonFile = new System.Windows.Forms.Button();
            this.button1 = new System.Windows.Forms.Button();
            this.TableCombo = new System.Windows.Forms.ComboBox();
            this.label3 = new System.Windows.Forms.Label();
            this.AutoMasterModeNone = new System.Windows.Forms.RadioButton();
            this.AutomasterModeAsVariables = new System.Windows.Forms.RadioButton();
            this.AutoMasterModeAsFields = new System.Windows.Forms.RadioButton();
            this.label4 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // TextServiceName
            // 
            this.TextServiceName.Enabled = false;
            this.TextServiceName.Location = new System.Drawing.Point(117, 61);
            this.TextServiceName.Name = "TextServiceName";
            this.TextServiceName.Size = new System.Drawing.Size(331, 20);
            this.TextServiceName.TabIndex = 0;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 64);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(74, 13);
            this.label1.TabIndex = 1;
            this.label1.Text = "Service Name";
            // 
            // textFilename
            // 
            this.textFilename.Location = new System.Drawing.Point(117, 27);
            this.textFilename.Name = "textFilename";
            this.textFilename.ReadOnly = true;
            this.textFilename.Size = new System.Drawing.Size(331, 20);
            this.textFilename.TabIndex = 2;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 30);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(68, 13);
            this.label2.TabIndex = 3;
            this.label2.Text = "Json Catalog";
            // 
            // buttonFile
            // 
            this.buttonFile.Location = new System.Drawing.Point(455, 27);
            this.buttonFile.Name = "buttonFile";
            this.buttonFile.Size = new System.Drawing.Size(24, 20);
            this.buttonFile.TabIndex = 4;
            this.buttonFile.Text = "...";
            this.buttonFile.UseVisualStyleBackColor = true;
            this.buttonFile.Click += new System.EventHandler(this.buttonFile_Click);
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(117, 163);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 23);
            this.button1.TabIndex = 5;
            this.button1.Text = "Design";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // TableCombo
            // 
            this.TableCombo.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.TableCombo.FormattingEnabled = true;
            this.TableCombo.Location = new System.Drawing.Point(117, 93);
            this.TableCombo.Name = "TableCombo";
            this.TableCombo.Size = new System.Drawing.Size(331, 21);
            this.TableCombo.TabIndex = 6;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 96);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(71, 13);
            this.label3.TabIndex = 7;
            this.label3.Text = "Data Member";
            // 
            // AutoMasterModeNone
            // 
            this.AutoMasterModeNone.AutoSize = true;
            this.AutoMasterModeNone.Checked = true;
            this.AutoMasterModeNone.Location = new System.Drawing.Point(118, 128);
            this.AutoMasterModeNone.Name = "AutoMasterModeNone";
            this.AutoMasterModeNone.Size = new System.Drawing.Size(49, 17);
            this.AutoMasterModeNone.TabIndex = 8;
            this.AutoMasterModeNone.TabStop = true;
            this.AutoMasterModeNone.Text = "none";
            this.AutoMasterModeNone.UseVisualStyleBackColor = true;
            // 
            // AutomasterModeAsVariables
            // 
            this.AutomasterModeAsVariables.AutoSize = true;
            this.AutomasterModeAsVariables.Location = new System.Drawing.Point(188, 128);
            this.AutomasterModeAsVariables.Name = "AutomasterModeAsVariables";
            this.AutomasterModeAsVariables.Size = new System.Drawing.Size(83, 17);
            this.AutomasterModeAsVariables.TabIndex = 9;
            this.AutomasterModeAsVariables.Text = "As Variables";
            this.AutomasterModeAsVariables.UseVisualStyleBackColor = true;
            // 
            // AutoMasterModeAsFields
            // 
            this.AutoMasterModeAsFields.AutoSize = true;
            this.AutoMasterModeAsFields.Location = new System.Drawing.Point(277, 128);
            this.AutoMasterModeAsFields.Name = "AutoMasterModeAsFields";
            this.AutoMasterModeAsFields.Size = new System.Drawing.Size(67, 17);
            this.AutoMasterModeAsFields.TabIndex = 10;
            this.AutoMasterModeAsFields.Text = "As Fields";
            this.AutoMasterModeAsFields.UseVisualStyleBackColor = true;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(13, 130);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(88, 13);
            this.label4.TabIndex = 11;
            this.label4.Text = "AutoMasterMode";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(496, 198);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.AutoMasterModeAsFields);
            this.Controls.Add(this.AutomasterModeAsVariables);
            this.Controls.Add(this.AutoMasterModeNone);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.TableCombo);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.buttonFile);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.textFilename);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.TextServiceName);
            this.Name = "Form1";
            this.Text = "OpenEdge Schema Tester";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox TextServiceName;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox textFilename;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button buttonFile;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.ComboBox TableCombo;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.RadioButton AutoMasterModeNone;
        private System.Windows.Forms.RadioButton AutomasterModeAsVariables;
        private System.Windows.Forms.RadioButton AutoMasterModeAsFields;
        private System.Windows.Forms.Label label4;
    }
}

