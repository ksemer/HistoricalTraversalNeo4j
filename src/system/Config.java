package system;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.Label;

/**
 * System's Configuration Class
 * 
 * @author ksemer
 */
public class Config {

	public static File DB_PATH;
	public static String DATABASE_CONFIG_PATH;
	// 1 = single points, 2 = multi edges, 3 = single new edges
	public static int TRAVERSAL_TYPE;
	public static Label NODE_LABEL;
	public static Label TIME_NODE_LABEL;
	public static String TIME_NODE_PROPERTY = "timeInstance";
	public static String QUERIES_PATH;
	public static String RESULTS_PATH;
	public static boolean SIMPLE_PATH;
	public static boolean RUN_PATH_QUERIES;
	public static boolean TIME_INDEX_ENABLED;

	private static final Logger _log = Logger.getLogger(Config.class.getName());

	public static void loadConfig() {
		final String SETTINGS_FILE = "./config/settings.properties";

		try {
			Properties Settings = new Properties();
			InputStream is = new FileInputStream(new File(SETTINGS_FILE));
			Settings.load(is);
			is.close();

			// ============================================================
			DB_PATH = new File(Settings.getProperty("DatabasePath", ""));
			DATABASE_CONFIG_PATH = Settings.getProperty("DatabaseConfigPath", "");
			TRAVERSAL_TYPE = Integer.parseInt(Settings.getProperty("traversalType", ""));
			QUERIES_PATH = Settings.getProperty("QueriesPath", "");
			RESULTS_PATH = Settings.getProperty("ResultsPath", "");
			SIMPLE_PATH = Boolean.parseBoolean(Settings.getProperty("findSimplePath", "false"));
			RUN_PATH_QUERIES = Boolean.parseBoolean(Settings.getProperty("runPathQueries", "false"));
			TIME_INDEX_ENABLED = Boolean.parseBoolean(Settings.getProperty("timeIndexEnable", "false"));
			NODE_LABEL = Label.label(Settings.getProperty("nodeLabel", "User"));
			TIME_NODE_LABEL = Label.label(Settings.getProperty("indexNodeLabel", "TimeInstance"));
			TIME_NODE_PROPERTY = Settings.getProperty("indexNodeProperty", "timeInstance");

			// ============================================================
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Failed to Load " + SETTINGS_FILE + " File.", e);
		}
	}
}