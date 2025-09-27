using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoETL.ETL.Mappings
{
    public class SurveyCsv
    {
        [Name("IdOpinion")]
        public string IdOpinion { get; set; }

        [Name("IdCliente")]
        public string IdCliente { get; set; }

        [Name("IdProducto")]
        public string IdProducto { get; set; }

        [Name("Fecha")]
        public DateTime Fecha { get; set; }

        [Name("Comentario")]
        public string Comentario { get; set; }

        [Name("Clasificación")] 
        public string Clasificacion { get; set; }

        [Name("PuntajeSatisfacción")]
        public int PuntajeSatisfaccion { get; set; }

        [Name("Fuente")]
        public string Fuente { get; set; }

    }
}
