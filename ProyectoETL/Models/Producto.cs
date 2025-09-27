using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoETL.Models
{
    public class Producto
    {
        public string IdProducto { get; set; }
        public string NombreProducto { get; set; }
        public int IdCategoria { get; set; }
    }
}
