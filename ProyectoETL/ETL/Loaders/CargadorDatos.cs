using Microsoft.Data.SqlClient;
using ProyectoETL.ETL.Mappings;
using ProyectoETL.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ProyectoETL.ETL.Loaders
{
    public class CargadorDatos
    {
        private readonly string _connectionString;

        public CargadorDatos(string connectionString)
        {
            _connectionString = connectionString;
        }

        public HashSet<string> GetValidClientIds()
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            return GetLookupSet("SELECT IdCliente FROM Clientes", connection);
        }

        public HashSet<string> GetValidProductIds()
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            return GetLookupSet("SELECT IdProducto FROM Productos", connection);
        }

        public void CargarDatosStaging(List<OpinionUnificada> opiniones, string nombreTabla)
        {
            Console.WriteLine($"Cargando {opiniones.Count} registros a la tabla de staging '{nombreTabla}'...");
            if (!opiniones.Any())
            {
                Console.WriteLine("No hay registros para cargar.");
                return;
            }

            var dt = new DataTable();
            dt.Columns.Add("IdFuenteOriginal", typeof(string));
            dt.Columns.Add("IdCliente", typeof(string));
            dt.Columns.Add("IdProducto", typeof(string));
            dt.Columns.Add("Fecha", typeof(DateTime));
            dt.Columns.Add("Comentario", typeof(string));

            // Columnas específicas por tabla
            if (nombreTabla == "SocialComments")
            {
                dt.Columns.Add("Fuente", typeof(string));
                foreach (var o in opiniones)
                {
                    if (o.IdCliente != "0")
                    {
                        dt.Rows.Add(o.IdFuenteOriginal, o.IdCliente, o.IdProducto, o.Fecha, o.Comentario, o.NombreCanal);
                    }
                }
            }
            else if (nombreTabla == "Surveys")
            {
                dt.Columns.Add("Clasificacion", typeof(string));
                dt.Columns.Add("PuntajeSatisfaccion", typeof(int));
                dt.Columns.Add("Fuente", typeof(string));
                foreach (var o in opiniones)
                {
                    dt.Rows.Add(o.IdFuenteOriginal, o.IdCliente, o.IdProducto, o.Fecha, o.Comentario, o.Clasificacion, o.PuntajeSatisfaccion, o.NombreCanal);
                }
            }
            else if (nombreTabla == "WebReviews")
            {
                dt.Columns.Add("Rating", typeof(int));
                foreach (var o in opiniones)
                {
                    dt.Rows.Add(o.IdFuenteOriginal, o.IdCliente, o.IdProducto, o.Fecha, o.Comentario, o.PuntajeSatisfaccion);
                }
            }

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = nombreTabla;
                    dt.Columns["IdFuenteOriginal"].ColumnName = nombreTabla switch
                    {
                        "SocialComments" => "IdComment",
                        "Surveys" => "IdOpinion",
                        "WebReviews" => "IdReview",
                        _ => dt.Columns["IdFuenteOriginal"].ColumnName
                    };

                    foreach (DataColumn col in dt.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                    bulkCopy.WriteToServer(dt);
                }
            }
        }

        public void CargarClientes(List<Cliente> clientes)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                foreach (var cliente in clientes)
                {
                    var sql = @"
                    IF NOT EXISTS (SELECT 1 FROM Clientes WHERE IdCliente = @IdCliente)
                    BEGIN
                        INSERT INTO Clientes (IdCliente, NombreCliente, Email) 
                        VALUES (@IdCliente, @NombreCliente, @Email);
                    END";
                    using (var command = new SqlCommand(sql, connection))
                    {
                        command.Parameters.AddWithValue("@IdCliente", cliente.IdCliente);
                        command.Parameters.AddWithValue("@NombreCliente", cliente.NombreCliente);
                        command.Parameters.AddWithValue("@Email", (object)cliente.Email ?? DBNull.Value);
                        command.ExecuteNonQuery();
                    }
                }
            }
            Console.WriteLine($"Carga de {clientes.Count} clientes finalizada.");
        }

        public void CargarTipoFuentesYCanales(List<OpinionUnificada> opiniones)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var tiposFuenteUnicos = opiniones.Select(o => o.NombreTipoFuente).Distinct().ToList();
                var mapaTiposFuente = new Dictionary<string, int>();

                foreach (var tipo in tiposFuenteUnicos)
                {
                    var sqlTipo = @"
                    IF NOT EXISTS (SELECT 1 FROM TipoFuentes WHERE NombreTipo = @Nombre)
                    BEGIN INSERT INTO TipoFuentes (NombreTipo) VALUES (@Nombre) END;
                    SELECT IdTipoFuente FROM TipoFuentes WHERE NombreTipo = @Nombre;";
                    using (var cmd = new SqlCommand(sqlTipo, connection))
                    {
                        cmd.Parameters.AddWithValue("@Nombre", tipo);
                        mapaTiposFuente[tipo] = (int)cmd.ExecuteScalar();
                    }
                }
                Console.WriteLine($"Cargados {mapaTiposFuente.Count} tipos de fuente.");

                var canalesUnicos = opiniones.Select(o => new { o.NombreCanal, o.NombreTipoFuente }).Distinct().ToList();
                foreach (var canalInfo in canalesUnicos)
                {
                    int idTipoFuente = mapaTiposFuente[canalInfo.NombreTipoFuente];
                    var sqlCanal = @"
                    IF NOT EXISTS (SELECT 1 FROM Canales WHERE NombreCanal = @Nombre)
                    BEGIN INSERT INTO Canales (NombreCanal, IdTipoFuente) VALUES (@Nombre, @IdTipo) END;";
                    using (var cmd = new SqlCommand(sqlCanal, connection))
                    {
                        cmd.Parameters.AddWithValue("@Nombre", canalInfo.NombreCanal);
                        cmd.Parameters.AddWithValue("@IdTipo", idTipoFuente);
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine($"Cargados {canalesUnicos.Count} canales.");
            }
        }

        public Dictionary<string, int> CargarCategoriasYObtenerMapa(List<ProductoCsv> productosCsv)
        {
            var categoriasUnicas = productosCsv.Select(p => p.Categoria.Trim()).Distinct().ToList();
            var mapaCategorias = new Dictionary<string, int>();

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                foreach (var nombreCategoria in categoriasUnicas)
                {
                    var sql = @"
                    IF NOT EXISTS (SELECT 1 FROM Categorias WHERE NombreCategoria = @Nombre)
                    BEGIN INSERT INTO Categorias (NombreCategoria) VALUES (@Nombre); END
                    SELECT IdCategoria FROM Categorias WHERE NombreCategoria = @Nombre;";
                    using (var command = new SqlCommand(sql, connection))
                    {
                        command.Parameters.AddWithValue("@Nombre", nombreCategoria);
                        mapaCategorias[nombreCategoria] = (int)command.ExecuteScalar();
                    }
                }
            }
            Console.WriteLine($"Cargadas {mapaCategorias.Count} categorías únicas.");
            return mapaCategorias;
        }

        public void CargarProductos(List<ProductoCsv> productosCsv, Dictionary<string, int> mapaCategorias)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                foreach (var pCsv in productosCsv)
                {
                    if (mapaCategorias.TryGetValue(pCsv.Categoria.Trim(), out int idCategoria))
                    {
                        var sql = @"
                        IF NOT EXISTS (SELECT 1 FROM Productos WHERE IdProducto = @IdProducto)
                        BEGIN
                            INSERT INTO Productos (IdProducto, NombreProducto, IdCategoria)
                            VALUES (@IdProducto, @NombreProducto, @IdCategoria);
                        END";
                        using (var command = new SqlCommand(sql, connection))
                        {
                            command.Parameters.AddWithValue("@IdProducto", pCsv.IdProducto);
                            command.Parameters.AddWithValue("@NombreProducto", pCsv.NombreProducto);
                            command.Parameters.AddWithValue("@IdCategoria", idCategoria);
                            command.ExecuteNonQuery();
                        }
                    }
                }
            }
            Console.WriteLine($"Carga de {productosCsv.Count} productos finalizada.");
        }

        public void CargarOpinionesUnificadas(List<OpinionUnificada> opiniones)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var clientIdsValidos = GetLookupSet("SELECT IdCliente FROM Clientes", connection);
                var productIdsValidos = GetLookupSet("SELECT IdProducto FROM Productos", connection);
                var canalesMap = GetLookupDictionary("SELECT NombreCanal, IdCanal FROM Canales", connection);
                var tipoFuentesMap = GetLookupDictionary("SELECT NombreTipo, IdTipoFuente FROM TipoFuentes", connection);

                var sqlCarga = "INSERT INTO CargasETL (NombreArchivo, RegistrosCargados) VALUES (@Nombre, 0); SELECT SCOPE_IDENTITY();";
                int idCarga = Convert.ToInt32(new SqlCommand(sqlCarga, connection) { Parameters = { new SqlParameter("@Nombre", "Carga General ETL") } }.ExecuteScalar());

                var dtOpiniones = new DataTable();
                dtOpiniones.Columns.Add("IdFuenteOriginal", typeof(string));
                dtOpiniones.Columns.Add("Comentario", typeof(string));
                dtOpiniones.Columns.Add("Fecha", typeof(DateTime));
                dtOpiniones.Columns.Add("Clasificacion", typeof(string));
                dtOpiniones.Columns.Add("PuntajeSatisfaccion", typeof(int));
                dtOpiniones.Columns.Add("IdProducto", typeof(string));
                dtOpiniones.Columns.Add("IdCliente", typeof(string));
                dtOpiniones.Columns.Add("IdCarga", typeof(int));
                dtOpiniones.Columns.Add("IdCanal", typeof(int));
                dtOpiniones.Columns.Add("IdTipoFuente", typeof(int));

                int registrosValidos = 0;
                foreach (var opinion in opiniones)
                {
                    if (string.IsNullOrEmpty(opinion.IdCliente) || opinion.IdCliente == "0" || !clientIdsValidos.Contains(opinion.IdCliente) ||
                        !productIdsValidos.Contains(opinion.IdProducto) ||
                        !canalesMap.ContainsKey(opinion.NombreCanal) ||
                        !tipoFuentesMap.ContainsKey(opinion.NombreTipoFuente))
                    {
                        continue;
                    }

                    dtOpiniones.Rows.Add(
                        opinion.IdFuenteOriginal,
                        opinion.Comentario,
                        opinion.Fecha,
                        (object)opinion.Clasificacion ?? DBNull.Value,
                        (object)opinion.PuntajeSatisfaccion ?? DBNull.Value,
                        opinion.IdProducto,
                        opinion.IdCliente,
                        idCarga,
                        canalesMap[opinion.NombreCanal],
                        tipoFuentesMap[opinion.NombreTipoFuente]
                    );
                    registrosValidos++;
                }

                if (registrosValidos > 0)
                {
                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = "Opiniones";
                        foreach (DataColumn col in dtOpiniones.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                        }
                        bulkCopy.WriteToServer(dtOpiniones);
                    }
                }

                var sqlUpdateCarga = "UPDATE CargasETL SET RegistrosCargados = @Registros WHERE IdCarga = @IdCarga;";
                using (var cmdUpdate = new SqlCommand(sqlUpdateCarga, connection))
                {
                    cmdUpdate.Parameters.AddWithValue("@Registros", registrosValidos);
                    cmdUpdate.Parameters.AddWithValue("@IdCarga", idCarga);
                    cmdUpdate.ExecuteNonQuery();
                }

                Console.WriteLine($"Carga en 'Opiniones' finalizada. Se insertaron {registrosValidos} de {opiniones.Count} registros leídos.");
            }
        }

        private HashSet<string> GetLookupSet(string query, SqlConnection connection)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using (var cmd = new SqlCommand(query, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read()) set.Add(reader.GetString(0));
            }
            return set;
        }

        private Dictionary<string, int> GetLookupDictionary(string query, SqlConnection connection)
        {
            var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            using (var cmd = new SqlCommand(query, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read()) dict[reader.GetString(0)] = reader.GetInt32(1);
            }
            return dict;
        }

        // Metodo para limpiar las tablas antes de una nueva carga
        public void LimpiarTablas()
        {
            Console.WriteLine("Limpiando tablas de destino para una carga nueva...");
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var sql = @"
                DELETE FROM Opiniones;
                DELETE FROM CargasETL;
                DELETE FROM Productos;
                DELETE FROM Categorias;
                DELETE FROM Clientes;
                DELETE FROM Canales;
                DELETE FROM TipoFuentes;
                DELETE FROM SocialComments;
                DELETE FROM Surveys;
                DELETE FROM WebReviews;
                
                DBCC CHECKIDENT ('Opiniones', RESEED, 0);
                DBCC CHECKIDENT ('CargasETL', RESEED, 0);
                DBCC CHECKIDENT ('Categorias', RESEED, 0);
                DBCC CHECKIDENT ('Canales', RESEED, 0);
                DBCC CHECKIDENT ('TipoFuentes', RESEED, 0);
            ";
                using (var command = new SqlCommand(sql, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
            Console.WriteLine("Tablas limpiadas.");
        }
    }
}