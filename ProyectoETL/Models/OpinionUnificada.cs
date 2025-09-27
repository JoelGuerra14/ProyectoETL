using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoETL.Models
{
    public class OpinionUnificada
    {
        public string IdFuenteOriginal { get; set; }
        public string Comentario { get; set; }
        public DateTime Fecha { get; set; }
        public string Clasificacion { get; set; }
        public int? PuntajeSatisfaccion { get; set; }
        public string IdProducto { get; set; }
        public string IdCliente { get; set; }
        public string NombreCanal { get; set; }
        public string NombreTipoFuente { get; set; }
    }
}
