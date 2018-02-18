using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary1
{
    public class Error : TableEntity
    {
        public Error(string link, string error)
        {

            this.PartitionKey = (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString();
            this.Link = link;
            this.ErrorMsg = error;
            this.RowKey = Guid.NewGuid().ToString();

        }

        public Error() { }
        public string Link { get; set; }
        public string ErrorMsg { get; set; }

    }
}
