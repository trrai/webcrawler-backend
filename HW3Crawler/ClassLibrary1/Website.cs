using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary1
{
    public class Website : TableEntity
    {
        public Website(string name, string address, string date)
        {
            this.PartitionKey = (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString();
            this.Name = name;
            this.Date = date;
            this.Address = address;
            this.RowKey = Guid.NewGuid().ToString();
        }

        public Website() { }
        public string Name { get; set; }
        public string Date { get; set; }
        public string Address { get; set; }
    }
}


