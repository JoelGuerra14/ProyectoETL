using System;

class Program
{
    static void Main(string[] args)
    {
        string connectionString = "Server=LAPTOP-2772BLAK\\SQLEXPRESS;Database=AnalisisOpiniones;Integrated Security=True;Encrypt=True;TrustServerCertificate=True";

        var procesador = new ProcesadorETL(connectionString);
        try
        {
            procesador.EjecutarProcesoCompleto();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("\nERROR: El proceso ETL falló.");
            Console.WriteLine(ex.ToString());
            Console.ResetColor();
        }
    }
}