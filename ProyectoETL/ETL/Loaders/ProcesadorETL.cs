using CsvHelper;
using CsvHelper.Configuration;
using ProyectoETL.ETL.Extractors;
using ProyectoETL.ETL.Loaders;
using ProyectoETL.ETL.Mappings;
using ProyectoETL.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

public class ProcesadorETL
{
    private readonly string _connectionString;
    private readonly CargadorDatos _cargador;
    private readonly string _rutaData;

    public ProcesadorETL(string connectionString)
    {
        _connectionString = connectionString;
        _cargador = new CargadorDatos(connectionString);

        string directorioBase = AppDomain.CurrentDomain.BaseDirectory;
        _rutaData = Path.GetFullPath(Path.Combine(directorioBase, "..", "..", "..", "Data"));
    }

    public void EjecutarProcesoCompleto()
    {
        Console.WriteLine("Iniciando Proceso ETL Completo...");

        // Limpieza inicial de la BD
        _cargador.LimpiarTablas();

        // Carga de Clientes y Productos
        Console.WriteLine("\n Cargando Dimensiones Maestras");
        var clientesCsv = LeerCsv<Cliente, ClienteMap>(Path.Combine(_rutaData, "clients.csv"));
        var productosCsv = LeerCsv<ProductoCsv, ProductoMap>(Path.Combine(_rutaData, "products.csv"));

        _cargador.CargarClientes(clientesCsv);
        var mapaCategorias = _cargador.CargarCategoriasYObtenerMapa(productosCsv);
        _cargador.CargarProductos(productosCsv, mapaCategorias);

        var validClientIds = _cargador.GetValidClientIds();
        var validProductIds = _cargador.GetValidProductIds();

        //Extracción, Transformación y Validación de datos de opiniones
        Console.WriteLine("\n Extrayendo y Transformando datos de opiniones");
        var opinionesUnificadas = new List<OpinionUnificada>();

        var socialExtractor = new SocialCommentExtractor(Path.Combine(_rutaData, "social_comments.csv"));
        var opinionesSocial = socialExtractor.ExtraerYTransformar();
        opinionesSocial.ForEach(o => o.NombreTipoFuente = "Red Social");
        opinionesUnificadas.AddRange(opinionesSocial);

        var surveyExtractor = new SurveyExtractor(Path.Combine(_rutaData, "surveys_part1.csv"));
        var opinionesSurvey = surveyExtractor.ExtraerYTransformar();

        Console.WriteLine($"\nValidando {opinionesSurvey.Count} encuestas crudas...");
        var opinionesSurveyLimpias = opinionesSurvey
            .Where(o => validClientIds.Contains(o.IdCliente) && validProductIds.Contains(o.IdProducto))
            .ToList();
        Console.WriteLine($" Se descartaron {opinionesSurvey.Count - opinionesSurveyLimpias.Count} encuestas por tener IDs de cliente/producto inválidos.");
        //Fin de validación

        opinionesSurveyLimpias.ForEach(o => o.NombreTipoFuente = "Encuesta");
        opinionesUnificadas.AddRange(opinionesSurveyLimpias);

        var webReviewExtractor = new WebReviewExtractor(Path.Combine(_rutaData, "web_reviews.csv"));
        var opinionesWeb = webReviewExtractor.ExtraerYTransformar();
        opinionesWeb.ForEach(o => o.NombreTipoFuente = "Web");
        opinionesUnificadas.AddRange(opinionesWeb);

        Console.WriteLine($"\nSe extrajeron un total de {opinionesUnificadas.Count} opiniones válidas.");

        // Carga de datos limpios a las tablas intermedias
        Console.WriteLine("\n Cargando datos a tablas de Staging ");
        _cargador.CargarDatosStaging(opinionesSocial, "SocialComments");
        _cargador.CargarDatosStaging(opinionesSurveyLimpias, "Surveys"); 
        _cargador.CargarDatosStaging(opinionesWeb, "WebReviews");

        Console.WriteLine("\n Cargando Dimensiones restantes a la BD ");
        _cargador.CargarTipoFuentesYCanales(opinionesUnificadas);

        //Carga de la Tabla de Opiniones
        Console.WriteLine("\n Cargando datos a la tabla de hechos 'Opiniones' ");
        _cargador.CargarOpinionesUnificadas(opinionesUnificadas);

        Console.WriteLine("\nProceso ETL finalizado con exito");
    }

    private List<T> LeerCsv<T>(string ruta)
    {
        using var reader = new StreamReader(ruta, System.Text.Encoding.UTF8);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        return csv.GetRecords<T>().ToList();
    }

    private List<T> LeerCsv<T, TMap>(string ruta) where TMap : ClassMap
    {
        using var reader = new StreamReader(ruta, System.Text.Encoding.UTF8);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        csv.Context.RegisterClassMap<TMap>();
        return csv.GetRecords<T>().ToList();
    }
}