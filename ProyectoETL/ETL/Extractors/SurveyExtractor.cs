using ProyectoETL.ETL.Interfaces;
using ProyectoETL.ETL.Mappings;
using ProyectoETL.Models;
using System.Collections.Generic;
using System.IO;
using CsvHelper;
using System.Globalization;
using System.Linq;

namespace ProyectoETL.ETL.Extractors
{
    public class SurveyExtractor : IExtractor
    {
        private readonly string _rutaArchivo;
        public SurveyExtractor(string rutaArchivo)
        {
            _rutaArchivo = rutaArchivo;
        }

        public List<OpinionUnificada> ExtraerYTransformar()
        {
            using var reader = new StreamReader(_rutaArchivo, System.Text.Encoding.UTF8);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<SurveyCsv>().ToList();

            var resultado = new List<OpinionUnificada>();
            foreach (var r in records)
            {
                if (string.IsNullOrWhiteSpace(r.Comentario)) continue;

                var opinion = new OpinionUnificada
                {
                    IdFuenteOriginal = r.IdOpinion.Trim(),
                    IdCliente = r.IdCliente.Trim(),
                    IdProducto = r.IdProducto.Trim(), 
                    Fecha = r.Fecha,
                    Comentario = r.Comentario.Trim(),
                    Clasificacion = r.Clasificacion.Trim(),
                    PuntajeSatisfaccion = r.PuntajeSatisfaccion,
                    NombreCanal = r.Fuente.Trim()
                };
                resultado.Add(opinion);
            }
            return resultado;
        }
    }
}