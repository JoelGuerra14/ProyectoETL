using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoETL.Models
{
    public class Canal
    {
        public int IdCanal { get; set; }
        public string NombreCanal { get; set; }
        public int IdTipoFuente { get; set; }
    }
}
