using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary1
{
    public class PerformanceStat : TableEntity
    {
        public PerformanceStat(int cpu, int mem)
        {
            this.PartitionKey = (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString();
            this.CPU = cpu;
            this.Memory = mem;
            this.RowKey = Guid.NewGuid().ToString();
        }

        public PerformanceStat() { }
        public int CPU { get; set; }
        public int Memory { get; set; }
    }
}
