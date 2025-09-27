using CsvHelper.Configuration;
using ProyectoETL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoETL.ETL.Mappings
{
    public class ClienteMap : ClassMap<Cliente>
    {
        public ClienteMap()
        {
            Map(m => m.IdCliente).Name("IdCliente");
            Map(m => m.NombreCliente).Name("Nombre");
            Map(m => m.Email).Name("Email");
        }
    }
}
