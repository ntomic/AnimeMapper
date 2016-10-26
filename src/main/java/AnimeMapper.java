import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import static java.nio.file.Files.isDirectory;

/**
 * Created by Nela on 10/4/2016.
 */

//TODO: add configuration file for db connections and storage info
//TODO: get multiple storage locations
//TODO: sync DB with current storage state
//TODO: add log

public class AnimeMapper
{

	static Connection databaseConnect() throws SQLException
	{
		String connectionString = "jdbc:mysql://localhost:3306/myanimedb?autoReconnect=true&useSSL=false";
		String connectionUserName = "root";
		String connectionPassword = "nelanela";

		Connection connection = null;
		try
		{
			connection = DriverManager.getConnection(connectionString, connectionUserName, connectionPassword);
			if(connection != null)
			{
				return connection;
			}
		} catch(SQLException ex)
		{
			if(!connection.isClosed()) connection.close();
			throw ex;
		}
		return null;
	}

	static void directoryWalk(Connection connection) throws SQLException
	{
		List<Path> directoryList = new ArrayList<Path>(){{
			add(Paths.get("E:/Anime"));
		}};

		for(Path item : directoryList)
			try(DirectoryStream<Path> stream = Files.newDirectoryStream(item))
			{
				for(Path file : stream)
				{
					if(isDirectory(file))
					{
						String animeName = file.getFileName().toString();
						int animeStorageID = 4;
						String query = "insert into anime (AnimeName, AnimeStorageID) values (?, ?)";

						try(PreparedStatement preparedStatement = connection.prepareStatement(query))
						{
							preparedStatement.setString(1, animeName);
							preparedStatement.setInt(2, animeStorageID);
							preparedStatement.executeUpdate();
						} catch(SQLException ex)
						{
							System.err.println(ex.getMessage());
						}
					}
				}
			} catch(IOException | DirectoryIteratorException err)
			{
				System.err.println(err);
			}

		if(!connection.isClosed()) connection.close();
	}

	public static void main(String[] args)
	{
		try
		{
			directoryWalk(databaseConnect());
		} catch(SQLException ex)
		{
			System.err.println(ex.getMessage());
		}
	}
}


