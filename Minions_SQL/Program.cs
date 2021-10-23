using System;
using System.Linq;
using SqlCommand = Microsoft.Data.SqlClient.SqlCommand;
using SqlConnection = Microsoft.Data.SqlClient.SqlConnection;
using SqlDataReader = Microsoft.Data.SqlClient.SqlDataReader;

namespace MinionsDB
{
    internal class Program
    {
        static string connectionString = "Server=.;Database=MinionsDB;Trusted_Connection=True";
        static void Main(string[] args)
        {
            Task2();
            Console.WriteLine("-------------------------------------------------------");
            Task3();
            Console.WriteLine("-------------------------------------------------------");
            Task4();
            Console.WriteLine("-------------------------------------------------------");
            Task5();
            Console.WriteLine("-------------------------------------------------------");
            Task6();
        }
        static void Task2()
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using (connection)
            {
               string selectionCommandString = "SELECT e.Name,COUNT(MinionID) FROM MinionsVillains AS b INNER JOIN Villains AS e ON b.VillainId=e.Id GROUP BY b.VillainId, e.Name HAVING COUNT(MinionId)>2 ORDER BY COUNT(MinionId) DESC";
               SqlCommand command = new SqlCommand(selectionCommandString, connection);
               SqlDataReader reader = command.ExecuteReader();
               using(reader)
               {
                   while (reader.Read())
                   {
                       for(int i = 0; i < reader.FieldCount; i++)
                       {
                           Console.Write($"{reader[i]} ");
                           if (i == 0)
                           {
                               Console.Write("- ");
                           }
                       }
                       Console.WriteLine();
                   }
               }
            }
        }

        static void Task3()
        {
            int id = int.Parse(Console.ReadLine());
            while (CheckIdVillain(id) == null) {Console.WriteLine($"No villain with ID {id} exists in the database."); id = Int32.Parse(Console.ReadLine()); };
            //CheckIdVillain(id);
            string villainName = GetVillainName(id);
            Console.WriteLine($"Villain: {villainName}");
            if (!CheckOfExistenceMinions(id))
            {
                Console.WriteLine("(no minions)");
            }
            else
            {
                SqlConnection connection = new SqlConnection(connectionString);
                connection.Open();
                using (connection)
                {
                   string selectionCommandString = $"SELECT Name, Age FROM Minions JOIN MinionsVillains AS e ON Minions.Id = e.MinionId WHERE e.VillainId = @id ORDER BY Minions.Name ASC";
                   //  string selectionCommandString = "SELECT Name, Age FROM Minions JOIN (SELECT MinionId FROM MinionsVillains WHERE VillainId=@id) AS b ON b.MinionId=Minions.Id ORDER BY Minions.Name";
                   SqlCommand command = new SqlCommand(selectionCommandString, connection); 
                   command.Parameters.AddWithValue("@id", id);
                   SqlDataReader reader = command.ExecuteReader(); 
                   int k = 1;
                   using(reader)
                   {
                       while (reader.Read()) 
                       { 
                           Console.WriteLine($"{k++}. {reader["Name"]} {reader["Age"]}");
                       }
                   } 
                }
                
            }
        }

        static void Task4()
        {
            Console.Write("Minion: ");
            string[] minion = Console.ReadLine().Split(' ');
            Console.Write("Villain: ");
            string villain = Console.ReadLine();
            string minionName = minion[0];
            int minionAge = Int32.Parse(minion[1]);
           // string minionTown = minion[2];
            string[] town = new string[minion.Length-2];
            for (int i=2; i < minion.Length; i++)
            {
                town[i-2] = minion[i];
            }

            string minionTown = string.Join(" ",town);
            if (GetTownId(minionTown) == 0)
            {
                InsertTown(minionTown);
            }

            if (GetVillainId(villain)== 0)
            {
                InsertVillain(villain);
            }

            int minionTownId = GetTownId(minionTown);
            InsertMinion(minionName,minionAge,minionTownId);
            int minionId = GetMinionId(minionName);
            int villainId= GetVillainId(villain);
            InsertSubmission(minionId, villainId);
            Console.WriteLine($"Успешно добавлен {minionName} чтобы быть миньоном {villain}.");
        }

        static void Task5()
        {
            int id = Int32.Parse(Console.ReadLine());
           
            while( CheckIdVillain(id)==null ){Console.WriteLine("Такой злодей не найден."); id = Int32.Parse(Console.ReadLine());}

            string villainName = GetVillainName(id);
            var minionsCount = DeleteSubmission(id);
            Console.WriteLine($"{minionsCount} миньонов было освобождено.");
            DeleteSubmission(id);
            Console.WriteLine($"{villainName} был удалён.");
            DeletVillain(id);
        }

        static void Task6()
        {
            string[] id = Console.ReadLine().Split(' ');
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using (connection)
            {
                for (int i = 0; i < id.Length; i++)
                {
                    string updateCommandString = "UPDATE Minions SET Age = Age + 1 WHERE Id = @id";
                    SqlCommand command = new SqlCommand(updateCommandString, connection);
                    command.Parameters.AddWithValue("@id", id[i]);
                    command.ExecuteNonQuery();
                }
                
            }
            ShowMinions(id);
        }

        //Проверка на существования злодея в базе данных по его id
        private static string CheckIdVillain(int id)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using(connection)
            {
                string selectionCommandString = "SELECT Name FROM Villains WHERE Id = @id";
                SqlCommand command = new SqlCommand(selectionCommandString, connection);
                //SqlParameter parameter = new SqlParameter("@id", SqlDbType.NVarInt) { Value = id };
                command.Parameters.AddWithValue("@id", id);
                string result = (string)command.ExecuteScalar();

                return result;
            }
        }
        
        //Проверка на наличие миньонов у злодея в базе данных
        private static bool CheckOfExistenceMinions(int id)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            string selectionCommandString = "SELECT COUNT(MinionID) FROM MinionsVillains WHERE VillainId = @id"; 
            SqlCommand command = new SqlCommand(selectionCommandString, connection); 
            connection.Open();
            using (connection)
            {
                command.Parameters.AddWithValue("@id", id);
                var result = (int)command.ExecuteScalar();
                return result > 0;
            }
        }
        
        // Получение id злодея по его имени
        private static int GetVillainId(string name)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            string selectionCommandString = "SELECT Id FROM Villains WHERE Name = @name"; 
            SqlCommand command = new SqlCommand(selectionCommandString, connection); 
            connection.Open();
            using (connection)
            {
                command.Parameters.AddWithValue("@name", name);
                int result = Convert.ToInt32(command.ExecuteScalar());
                return result;
            }
        }
        
        //Получение имени злодея по его id
        private static string GetVillainName(int id)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            string selectionCommandString = "SELECT Name FROM Villains WHERE Id = @id"; 
            SqlCommand command = new SqlCommand(selectionCommandString, connection); 
            connection.Open();
            using (connection)
            {
                command.Parameters.AddWithValue("@id", id); 
                string result = Convert.ToString(command.ExecuteScalar());
                return result;
            }
        }
       
        //Получение id города
        private static int GetTownId(string townName)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            string selectionCommandString = "SELECT Id FROM Towns WHERE Name = @townName"; 
            SqlCommand command = new SqlCommand(selectionCommandString, connection); 
            connection.Open();
            using (connection)
            {
                command.Parameters.AddWithValue("@townName", townName);
                int result = Convert.ToInt32(command.ExecuteScalar());
                return result;
            }
        }
        
        //Получение id миньона
        private static int GetMinionId(string minionName)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            string selectionCommandString = "SELECT Id FROM Minions WHERE Name = @minionName"; 
            SqlCommand command = new SqlCommand(selectionCommandString, connection); 
            connection.Open();
            using (connection)
            {
                command.Parameters.AddWithValue("@minionName", minionName);
                int result = Convert.ToInt32(command.ExecuteScalar());
                return result;
            }
        }
        
        //Добавление связи подчинения в базу данных
        static void InsertSubmission(int minionId, int villainId)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using(connection)
            {
                SqlCommand insertCommand = new SqlCommand("INSERT INTO MinionsVillains " +
                                                "(MinionId, VillainId) VALUES " +
                                                "(@minionId, @villainId)", connection);

                insertCommand.Parameters.AddWithValue("@minionId", minionId);
                insertCommand.Parameters.AddWithValue("@villainId", villainId);
               

                insertCommand.ExecuteNonQuery();
            }
        }
        
        //Добавление миньона в базу данных
        static void InsertMinion(string name,int age,int townId)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using(connection)
            {
                SqlCommand insertCommand = new SqlCommand("INSERT INTO Minions " +
                                                "(Name, Age, TownId) VALUES " +
                                                "(@name, @age, @townId)", connection);

                insertCommand.Parameters.AddWithValue("@name", name);
                insertCommand.Parameters.AddWithValue("@age", age);
                insertCommand.Parameters.AddWithValue("@townId", townId);

                insertCommand.ExecuteNonQuery();
            }
        }
        
        //Добавление города в базу данных
        static void InsertTown(string name)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using(connection)
            {
                SqlCommand insertCommand = new SqlCommand("INSERT INTO Towns " +
                                                "(Name, CountryCode) VALUES " +
                                                "(@name, 1)", connection);

                insertCommand.Parameters.AddWithValue("@name", name);
                insertCommand.ExecuteNonQuery();
                Console.WriteLine($"Город {name} был добавлен в базу данных.");
            }
        }
        
        //Добавление злодея в базу данных
        static void InsertVillain(string name)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using(connection)
            {
                SqlCommand insertCommand = new SqlCommand("INSERT INTO Villains " +
                                                "(Name, EvilnessFactorId) VALUES " +
                                                "(@name, 4)", connection);

                insertCommand.Parameters.AddWithValue("@name", name);
                insertCommand.ExecuteNonQuery();
                Console.WriteLine($"Злодей {name} был добавлен в базу данных.");
            }
        }
        
        //Освобождение миньона от подчинения
        private static int DeleteSubmission(int id)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using(connection)
            {
                SqlCommand deleteCommand = new SqlCommand("DELETE FROM MinionsVillains WHERE VillainId = @id", connection);
                deleteCommand.Parameters.AddWithValue("@id", id);
                return deleteCommand.ExecuteNonQuery();
            }
        }
        
        //Удаление злодея
        static void DeletVillain(int id)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using(connection)
            {
                string deleteCommand = $"DELETE FROM Villains WHERE Id = @id";
                SqlCommand command = new SqlCommand(deleteCommand, connection);
                command.Parameters.AddWithValue("@id", id);
                command.ExecuteNonQuery();
                
            }
        }
        
        //Увеличение возраста миньона на 1 ед
        static void ShowMinions(string[] id)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            using (connection)
            {
                for (int i = 0; i < id.Length; i++)
                {
                    string selectCommandString = "SELECT Name, Age FROM Minions WHERE Id=@id";
                    SqlCommand command = new SqlCommand(selectCommandString, connection);
                    command.Parameters.AddWithValue("@id", id[i]);
                    SqlDataReader reader = command.ExecuteReader();
                    using(reader)
                    {
                        while (reader.Read()) 
                        { 
                            for (int j = 0; j < reader.FieldCount; j++)
                            {
                                Console.Write($"{reader[j]}");
                            }
                            Console.WriteLine();
                        }
                    } 
                }
                
            }
        }
    }
    
}