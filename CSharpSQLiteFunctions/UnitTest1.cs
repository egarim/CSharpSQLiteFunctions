using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;

namespace CSharpSQLiteFunctions;

public class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void AggregatedFunction()
    {
        // Create an in-memory SQLite database
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        // Create and register the 'stdev' aggregate function
        connection.CreateAggregate(
            "stdev",
            (Count: 0, Sum: 0.0, SumOfSquares: 0.0),
            ((int Count, double Sum, double SumOfSquares) context, double value) => {
                context.Count++;
                context.Sum += value;
                context.SumOfSquares += value * value;
                return context;
            },
            context => {
                var variance = context.SumOfSquares - context.Sum * context.Sum / context.Count;
                return Math.Sqrt(variance / context.Count);
            });

        // Create a student table
        var createTableCmd = connection.CreateCommand();
        createTableCmd.CommandText = @"
            CREATE TABLE student (
                id INTEGER PRIMARY KEY,
                gpa DOUBLE
            );
        ";
        createTableCmd.ExecuteNonQuery();

        // Insert sample data into the student table
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
            INSERT INTO student (gpa) VALUES (3.5);
            INSERT INTO student (gpa) VALUES (3.7);
            INSERT INTO student (gpa) VALUES (2.8);
            INSERT INTO student (gpa) VALUES (3.9);
            INSERT INTO student (gpa) VALUES (3.2);
        ";
        insertCmd.ExecuteNonQuery();

        // Execute the query to calculate the standard deviation of GPA
        var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT stdev(gpa)
            FROM student
        ";
        var stdDev = (double)command.ExecuteScalar();
        
        Debug.WriteLine($"Standard Deviation of GPA: {stdDev}");
        
        Assert.AreEqual(0.38678159211627366, stdDev);
    }

    
    [Test]
    public void ScalarFunctionTest()
    {
        // Create an in-memory SQLite database
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        // Create and register the 'volume' function
        connection.CreateFunction("volume", (double radius, double height) => 
            Math.PI * Math.Pow(radius, 2) * height);

        // Create a cylinder table
        var createTableCmd = connection.CreateCommand();
        createTableCmd.CommandText = @"
            CREATE TABLE cylinder (
                id INTEGER PRIMARY KEY,
                name TEXT,
                radius DOUBLE,
                height DOUBLE
            );
        ";
        createTableCmd.ExecuteNonQuery();

        // Insert sample data into the cylinder table
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
            INSERT INTO cylinder (name, radius, height) VALUES ('Cylinder A', 2, 10);
            INSERT INTO cylinder (name, radius, height) VALUES ('Cylinder B', 3, 15);
            INSERT INTO cylinder (name, radius, height) VALUES ('Cylinder C', 1, 5);
        ";
        insertCmd.ExecuteNonQuery();

        // Execute the query to select cylinder data
        var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT name, volume(radius, height) AS volume
            FROM cylinder
            ORDER BY volume DESC
        ";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            Debug.WriteLine($"Name: {reader["name"]}, Volume: {reader["volume"]}");
        }
    }
    [Test]
    public void OperatorsTest()
    {
        // Create an in-memory SQLite database
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        // Create a user table
        var createTableCmd = connection.CreateCommand();
        createTableCmd.CommandText = @"
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                bio TEXT
            );
        ";
        createTableCmd.ExecuteNonQuery();

        // Insert sample data into the user table
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
            INSERT INTO user (bio) VALUES ('John D.  Smith');
            INSERT INTO user (bio) VALUES ('Jane Doe');
            INSERT INTO user (bio) VALUES ('Alice A.  Johnson');
            INSERT INTO user (bio) VALUES ('Bob');
        ";
        insertCmd.ExecuteNonQuery();

        // Define and register the REGEXP function
        connection.CreateFunction("regexp", (string pattern, string input) =>
        {
            return System.Text.RegularExpressions.Regex.IsMatch(input, pattern);
        });

        // Execute the query to count users matching the regex
        var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT count(*)
            FROM user
            WHERE bio REGEXP '\w\. {2,}\w'
        ";
        var count = (long)command.ExecuteScalar();

        Console.WriteLine($"Count of users matching regex: {count}");
        
        
        Assert.AreEqual(2, count);
    }
}