using ProyectoETL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoETL.ETL.Interfaces
{
    public interface IExtractor
    {
        List<OpinionUnificada> ExtraerYTransformar();
    }
}
