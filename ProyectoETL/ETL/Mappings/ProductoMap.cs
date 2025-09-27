using CsvHelper.Configuration;
using ProyectoETL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoETL.ETL.Mappings
{
    public class ProductoMap : ClassMap<ProductoCsv>
    {
        public ProductoMap()
        {
            Map(m => m.IdProducto).Name("IdProducto");
            Map(m => m.NombreProducto).Name("Nombre");
            Map(m => m.Categoria).Name("Categoría"); 
        }
    }
}
