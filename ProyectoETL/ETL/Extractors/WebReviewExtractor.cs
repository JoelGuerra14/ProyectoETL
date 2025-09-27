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
    public class WebReviewExtractor : IExtractor
    {
        private readonly string _rutaArchivo;
        public WebReviewExtractor(string rutaArchivo)
        {
            _rutaArchivo = rutaArchivo;
        }

        public List<OpinionUnificada> ExtraerYTransformar()
        {
            using var reader = new StreamReader(_rutaArchivo, System.Text.Encoding.UTF8);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<WebReviewCsv>().ToList();

            var resultado = new List<OpinionUnificada>();
            foreach (var r in records)
            {
                if (string.IsNullOrWhiteSpace(r.Comentario)) continue;

                var opinion = new OpinionUnificada
                {
                    IdFuenteOriginal = r.IdReview,
                    IdCliente = NormalizarId(r.IdCliente),
                    IdProducto = NormalizarId(r.IdProducto),
                    Fecha = r.Fecha,
                    Comentario = r.Comentario.Trim(),
                    Clasificacion = ConvertirRatingAClasificacion(r.Rating),
                    PuntajeSatisfaccion = r.Rating,
                    NombreCanal = "Web"
                };
                resultado.Add(opinion);
            }
            return resultado;
        }

        // Metodo para normalizar IDs con letras al inicio
        private string NormalizarId(string idCsv)
        {
            if (string.IsNullOrWhiteSpace(idCsv)) return null;
            string idTrimmed = idCsv.Trim();
            if (char.IsLetter(idTrimmed[0]))
            {
                string parteNumerica = new string(idTrimmed.Skip(1).ToArray());
                if (int.TryParse(parteNumerica, out int idNumerico))
                {
                    return idNumerico.ToString();
                }
            }
            return idTrimmed;
        }

        // Metodo para transformar el puntaje numérico a texto
        private string ConvertirRatingAClasificacion(int rating)
        {
            if (rating >= 4) return "Positiva";
            if (rating == 3) return "Neutra";
            return "Negativa";
        }
    }
}