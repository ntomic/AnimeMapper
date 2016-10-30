import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static java.nio.file.Files.isDirectory;

/**
 * Created by Nela on 10/4/2016.
 */


//TODO: add log
//TODO: check if anime on disc deleted or moved ... db to list; for every anime on disc remove from list ?

public class AnimeMapper
{
	private static Properties properties = new Properties();
	private static final String propertiesFile = "config.properties";

	private static void loadProperties() throws IOException
	{
		try(InputStream resourceStream = AnimeMapper.class.getResourceAsStream(propertiesFile))
		{
			properties.load(resourceStream);
		}
	}


	static Connection databaseConnect() throws SQLException, IOException
	{
		String connectionString = properties.getProperty("connectionStringProperty");
		String connectionUserName = properties.getProperty("connectionUserNameProperty");
		String connectionPassword = properties.getProperty("connectionPasswordProperty");

		Connection connection = null;
		try
		{
			connection = DriverManager.getConnection(connectionString, connectionUserName, connectionPassword);
			if(connection != null)
			{
				return connection;
			}
		}
		catch(SQLException ex)
		{
			if(!connection.isClosed())
				connection.close();
			throw ex;
		}
		return null;
	}

	static int getAnimeID (String animeTitle, Connection connection)
	{
		String querySelectAnime = "select Anime.AnimeID from Anime where AnimeName = ?";

		try(PreparedStatement preparedStatement = connection.prepareStatement(querySelectAnime))
		{
			preparedStatement.setString(1, animeTitle);
			ResultSet resultSet = preparedStatement.executeQuery();
			if(resultSet.isBeforeFirst())
			{
				resultSet.next();
				return resultSet.getInt(1);
			}
		}
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());

		}
		return -1;
	}


	static Boolean isInStorage(String animeTitle, int storageID, Connection connection)
	{
		String querySelectAnime = "select Anime.AnimeName from Anime inner join Anime_Storage on Anime.AnimeID = Anime_Storage.AnimeID where AnimeName = ? and Anime.AnimeID = ? ";

		try(PreparedStatement preparedStatement = connection.prepareStatement(querySelectAnime))
		{
			preparedStatement.setString(1, animeTitle);
			preparedStatement.setInt(2, storageID);
			ResultSet resultSet = preparedStatement.executeQuery();
			if(resultSet.isBeforeFirst())
				return true;
		}
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());
		}
		return false;
	}

	static int insertIntoAnime(String animeTitle, Connection connection)
	{
		String queryInsertAnime = "insert into anime (AnimeName) values (?)";

		try(PreparedStatement preparedStatement = connection.prepareStatement(queryInsertAnime,Statement.RETURN_GENERATED_KEYS))
		{
			preparedStatement.setString(1, animeTitle);
			preparedStatement.executeUpdate();


			ResultSet resultSet = preparedStatement.getGeneratedKeys();
			resultSet.next();
			return resultSet.getInt(1);
		}
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());
		}
		return -1;
	}

	static Boolean insertIntoAnimeStorage(int animeID, int storageID, Connection connection)
	{
		String queryInsertAnimeStorage = "insert into Anime_Storage (AnimeID, StorageID) values (?,?)";

		try(PreparedStatement preparedStatement = connection.prepareStatement(queryInsertAnimeStorage))
		{
			preparedStatement.setInt(1, animeID);
			preparedStatement.setInt(2, storageID);
			preparedStatement.executeUpdate();
		}
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());
			return false;
		}
		return true;
	}

	static Map<Integer, Path> getStorageList(Connection connection) {
		String queryGetStorage = "select StorageID, StorageDirectoryPath from Storage";
		Map<Integer, Path> storageList = new HashMap<>();
		try(Statement statementStorage = connection.createStatement()

		)
		{
			ResultSet resultSetStorage = statementStorage.executeQuery(queryGetStorage);

			while(resultSetStorage.next())
			{
				storageList.put(resultSetStorage.getInt(1), Paths.get(resultSetStorage.getString(2)));
			}
		}
		catch(SQLException ex)
		{
			System.err.println(ex.getMessage());
		}
		return storageList;
	}

	static void directoryWalk(Connection connection) throws SQLException
	{
		Map<Integer, Path> storageList = getStorageList(connection);

		for(Map.Entry<Integer, Path> disc : storageList.entrySet())
			try(DirectoryStream<Path> directoryStream = Files.newDirectoryStream(disc.getValue()))
			{
				for(Path file : directoryStream)
				{
					if(isDirectory(file))
					{


						String animeName = file.getFileName().toString();
						int animeStorageID = disc.getKey();


						int animeID = getAnimeID(animeName, connection);

						if(animeID > 0)
						{
							if(!isInStorage(animeName, animeStorageID, connection))
							{
								insertIntoAnimeStorage(animeID, animeStorageID, connection);

							}
						}
						else
						{
							animeID = insertIntoAnime(animeName,connection);
							insertIntoAnimeStorage(animeID, animeStorageID, connection);
						}

					}
				}
			}
			catch(IOException | DirectoryIteratorException ex)
			{
				System.err.println(ex);
			}

		if(!connection.isClosed())
			connection.close();
	}


	public static void main(String[] args)
	{
		try
		{
			loadProperties();
			directoryWalk(databaseConnect());
		}
		catch(IOException | SQLException ex)
		{
			System.err.println(ex.getMessage() + " MAIN: ");
		}
	}
}


