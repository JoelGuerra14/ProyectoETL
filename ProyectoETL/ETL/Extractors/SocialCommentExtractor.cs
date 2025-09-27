using ProyectoETL.ETL.Interfaces;
using ProyectoETL.ETL.Mappings;
using ProyectoETL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using CsvHelper;
using System.Globalization;

namespace ProyectoETL.ETL.Extractors
{
    public class SocialCommentExtractor : IExtractor
    {
        private readonly string _rutaArchivo;
        public SocialCommentExtractor(string rutaArchivo)
        {
            _rutaArchivo = rutaArchivo;
        }

        public List<OpinionUnificada> ExtraerYTransformar()
        {
            using var reader = new StreamReader(_rutaArchivo, System.Text.Encoding.UTF8);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<SocialCommentCsv>().ToList();

            var resultado = new List<OpinionUnificada>();
            foreach (var r in records)
            {
                if (string.IsNullOrWhiteSpace(r.Comentario) || string.IsNullOrWhiteSpace(r.IdProducto)) continue;

                var opinion = new OpinionUnificada
                {
                    IdFuenteOriginal = r.IdComment,
                    IdCliente = NormalizarIdCliente(r.IdCliente),
                    IdProducto = NormalizarIdProducto(r.IdProducto),
                    Fecha = r.Fecha,
                    Comentario = r.Comentario.Trim(),
                    Clasificacion = ClasificarSentimiento(r.Comentario),
                    PuntajeSatisfaccion = null,
                    NombreCanal = r.Fuente.Trim()
                };
                resultado.Add(opinion);
            }
            return resultado;
        }

        // Metodo encargado de normalizar el Id del cliente
        private string NormalizarIdCliente(string idClienteCsv)
        {

            if (string.IsNullOrWhiteSpace(idClienteCsv))
            {
                return null;
            }

            string idTrimmed = idClienteCsv.Trim();

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

        // Metodo encargado de normalizar el Id del producto
        private string NormalizarIdProducto(string idProductoCsv)
        {
            if (string.IsNullOrWhiteSpace(idProductoCsv))
            {
                return null;
            }

            string idTrimmed = idProductoCsv.Trim();

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

        // Revisar comentarios para ver si tienen palabras positivas o negativas
        private string ClasificarSentimiento(string comentario)
        {
            string texto = comentario.ToLower();
            if (texto.Contains("bueno") || texto.Contains("gran") || texto.Contains("excelente") || texto.Contains("perfecto") || texto.Contains("superior")) return "Positiva";
            if (texto.Contains("malo") || texto.Contains("rompió") || texto.Contains("terrible") || texto.Contains("insatisfecho") || texto.Contains("decepcionado")) return "Negativa";
            return "Neutra";
        }
    }
}