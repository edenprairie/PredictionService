using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;
using System.Xml.Serialization;

namespace Novologix.Prediction.Service
{
    public class SqlDAL
    {
        public SqlDAL()
        {

        }

        public SqlDAL(string ConnectionString):this()
        {
            this.ConnectionString = ConnectionString;
        }
        public string ConnectionString { get; set; }

        private string GetConnectionString()
        {
            if (ConnectionString == null)
                throw new ArgumentNullException("ConnectionString", "Subscribe to the static event OnSetConnectionString on DAL class to set the connection string");
            return ConnectionString;
        }

        private void AddParameters(SqlCommand com, SqlParameter[] parameters)
        {
            if (com == null)
                return;
            if (parameters == null)
                return;
            foreach (SqlParameter pr in parameters)
            {
                if (pr.Value == null)
                    pr.Value = DBNull.Value;
                com.Parameters.Add(pr);
            }
        }

        public DataSet GetDataSet(string Command, params SqlParameter[] parameters)
        {
            DataSet reDt = new DataSet();
            string ConnectionString = GetConnectionString();
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.CommandTimeout = 0;
                com.Connection = con;
                com.CommandText = Command;
                AddParameters(com, parameters);
                SqlDataAdapter DA = new SqlDataAdapter(com);
                DA.Fill(reDt);
            }
            return reDt;
        }

        public DataTable GetDataTable(string Command)
        {
            return GetDataTable(Command, null);
        }

        public DataTable GetDataTable(string Command, params SqlParameter[] parameters)
        {
            DataTable reDt = new DataTable();
            string ConnectionString = GetConnectionString();
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.CommandTimeout = 0;
                com.Connection = con;
                com.CommandText = Command;
                AddParameters(com, parameters);
                SqlDataAdapter DA = new SqlDataAdapter(com);
                DA.Fill(reDt);
            }
            return reDt;
        }

        public DataTable GetDataTable(string Command, CommandType commandType, params SqlParameter[] parameters)
        {
            DataTable reDt = new DataTable();
            string ConnectionString = GetConnectionString();
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.CommandTimeout = 6000; // 10 minutes
                com.Connection = con;
                com.CommandText = Command;
                com.CommandType = commandType;
                AddParameters(com, parameters);
                SqlDataAdapter DA = new SqlDataAdapter(com);
                DA.Fill(reDt);
            }
            return reDt;
        }

        public DataTable GetDataTable(string Command, int StartIndex, int NoOfRecords, params SqlParameter[] parameters)
        {
            DataTable reDt = null;
            DataSet filler = new DataSet();
            string ConnectionString = GetConnectionString();
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.CommandTimeout = 0;
                com.Connection = con;
                com.CommandText = Command;
                AddParameters(com, parameters);
                SqlDataAdapter DA = new SqlDataAdapter(com);
                DA.Fill(filler, StartIndex, NoOfRecords, "Table 1");
                if (filler != null)
                    if (filler.Tables.Count > 0)
                        reDt = filler.Tables[0];
            }
            return reDt;
        }

        public object GetScalar(string Command)
        {
            return GetScalar(Command, null);
        }

        public object GetScalar(string Command, params SqlParameter[] parameters)
        {
            string ConnectionString = GetConnectionString();
            object retval = null;
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.Connection = con;
                com.CommandText = Command;
                com.CommandTimeout = 0;
                AddParameters(com, parameters);
                con.Open();
                retval = com.ExecuteScalar();
                con.Close();
            }
            return retval;
        }

        public string GetString(string Command)
        {
            return GetString(Command, null);
        }

        public string GetString(string Command, params SqlParameter[] parameters)
        {
            object obj = GetScalar(Command, parameters);
            return Convert.ToString(obj);
        }

        public int GetInteger(string Command)
        {
            return GetInteger(Command, null);
        }

        public int GetInteger(string Command, params SqlParameter[] parameters)
        {
            object obj = GetScalar(Command, parameters);
            return Convert.ToInt32(obj);
        }

        public decimal GetDecimal(string Command)
        {
            return GetDecimal(Command, null);
        }

        public decimal GetDecimal(string Command, params SqlParameter[] parameters)
        {
            object obj = GetScalar(Command, parameters);
            return Convert.ToDecimal(obj);
        }

        public bool GetBool(string Command)
        {
            return GetBool(Command, null);
        }

        public bool GetBool(string Command, params SqlParameter[] parameters)
        {
            object obj = GetScalar(Command, parameters);
            return Convert.ToBoolean(obj);
        }

        public DateTime GetDate(string Command)
        {
            return GetDate(Command, null);
        }

        public DateTime GetDate(string Command, params SqlParameter[] parameters)
        {
            object obj = GetScalar(Command, parameters);
            return Convert.ToDateTime(obj);
        }

        public T GetObject<T>(string Command) where T : class
        {
            return GetObject<T>(Command, null);
        }

        public T GetObject<T>(string Command, params SqlParameter[] parameters) where T : class
        {
            string ConnectionString = GetConnectionString();
            T retval = default(T);
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.CommandTimeout = 0;
                com.Connection = con;
                com.CommandText = Command;
                AddParameters(com, parameters);
                con.Open();
                SqlDataReader DR = com.ExecuteReader();

                if (DR.HasRows)
                {
                    Type ty = typeof(T);
                    PropertyInfo[] PIS = ty.GetProperties().Where(c => c.GetCustomAttribute(typeof(DALIgnore)) == null).ToArray();
                    retval = ((T)Activator.CreateInstance(ty));
                    while (DR.Read())
                    {
                        foreach (PropertyInfo pi in PIS)
                        {
                            pi.SetValue(retval, DR[pi.Name]);
                        }
                        break;
                    }
                }
                con.Close();
            }
            return retval as T;
        }

        public List<T> GetListObject<T>(string Command)
        {
            return GetListObject<T>(Command, null);
        }

        public List<string> GetStringList(string Command)
        {
            List<string> retval = new List<string>();
            string conString = null;
            SqlConnection con;
            SqlCommand cmd;
            string sql = null;
            SqlDataReader reader;

            conString = GetConnectionString();
            sql = Command;

            con = new SqlConnection(conString);
            try
            {
                con.Open();
                cmd = new SqlCommand(sql, con);
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    retval.Add((string)reader.GetValue(0));
         
                }
                reader.Close();
                cmd.Dispose();
                con.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return retval;
        }


        public List<T> GetListObject<T>(string Command, params SqlParameter[] parameters)
        {
            string ConnectionString = GetConnectionString();
            List<T> retval = new List<T>();
                using (SqlConnection con = new SqlConnection(ConnectionString))
                {
                    SqlCommand com = new SqlCommand();
                    com.Connection = con;
                    com.CommandText = Command;
                    com.CommandTimeout = 0;
                    AddParameters(com, parameters);
                    con.Open();
                    SqlDataReader DR = com.ExecuteReader();

                    if (DR.HasRows)
                    {
                        Type ty = typeof(T);
                        if (!ty.IsPrimitive)
                        {
                            PropertyInfo[] PIS = ty.GetProperties().Where(c => c.GetCustomAttribute(typeof(DALIgnore)) == null).ToArray();
                            while (DR.Read())
                            {
                                T item = ((T)Activator.CreateInstance(ty));
                                foreach (PropertyInfo pi in PIS)
                                {
                                    pi.SetValue(item, ((DR[pi.Name] is DBNull) ? null : DR[pi.Name]));
                                }
                                retval.Add(item);
                            }
                        }
                        else
                        {
                            while (DR.Read())
                            {
                                retval.Add((T)DR[0]);
                            }
                        }

                    }
                    con.Close();
                }
            return retval as List<T>;
        }

        public List<T> GetListObject<T>(string Command, int StartIndex, int NoOfRecords, params SqlParameter[] parameters)
        {
            List<T> retval = new List<T>();
            DataTable dt = GetDataTable(Command, StartIndex, NoOfRecords, parameters);
            Type ty = typeof(T);
            PropertyInfo[] PIS = ty.GetProperties().Where(c => c.GetCustomAttribute(typeof(DALIgnore)) == null).ToArray();
            foreach (DataRow Dr in dt.Rows)
            {
                T item = ((T)Activator.CreateInstance(ty));
                foreach (PropertyInfo pi in PIS)
                {
                    pi.SetValue(item, Dr[pi.Name]);
                }
                retval.Add(item);
            }
            dt.Dispose();
            dt = null;
            return retval as List<T>;

        }

        public int GetExecuteNonQuery(string Command, params SqlParameter[] parameters)
        {
            string ConnectionString = GetConnectionString();
            int retval = 0;
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.Connection = con;
                com.CommandText = Command;
                AddParameters(com, parameters);
                con.Open();
                com.CommandTimeout = 0;
                retval = com.ExecuteNonQuery();
                con.Close();
            }
            return retval;
        }

        public IDataReader GetReader(string Command, params SqlParameter[] parameters)
        {
            string ConnectionString = GetConnectionString();
            SqlConnection con = new SqlConnection(ConnectionString);
            SqlCommand com = new SqlCommand();
            com.CommandTimeout = 0;
            com.Connection = con;
            com.CommandText = Command;
            AddParameters(com, parameters);
            con.Open();
            SqlDataReader DR = com.ExecuteReader(CommandBehavior.CloseConnection);
            return DR;
        }


        //public int DoBulkCopy<T>(IEnumerable<T> Collection, string TableName, int BatchSize = 10000, int timeout = 0, bool EnableStreaming = false)
        //{
        //    string ConnectionString = GetConnectionString();
        //    ListToDataReader<T> dr = new ListToDataReader<T>((IEnumerable<T>)Collection);
        //    using (SqlBulkCopy blk = new SqlBulkCopy(ConnectionString))
        //    {
        //        blk.BatchSize = BatchSize;
        //        blk.BulkCopyTimeout = timeout;
        //        blk.DestinationTableName = TableName;
        //        blk.EnableStreaming = EnableStreaming;
        //        blk.WriteToServer(dr);
        //    }
        //    return 0;
        //}

        public void ExecuteNonQuery(string commandText, CommandType commandType, params SqlParameter[] parameters)
        {
            string ConnectionString = GetConnectionString();
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                SqlCommand com = new SqlCommand();
                com.CommandTimeout = 0;
                com.Connection = con;
                com.CommandText = commandText;
                com.CommandType = commandType;
                AddParameters(com, parameters);
                
                try
                {
                    con.Open();
                    com.ExecuteNonQuery();
                }
                finally
                {
                    if (con != null && con.State != ConnectionState.Closed)
                        con.Close();
                }
            }
        }

        
    }

    [AttributeUsage(AttributeTargets.All, Inherited = false, AllowMultiple = true)]
    public sealed class DALIgnore : Attribute
    {

    }
}
