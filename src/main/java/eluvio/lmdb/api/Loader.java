package eluvio.lmdb.api;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;
import jnr.ffi.Platform;

class Loader {
  final private static String ENV_VARIABLE_NAME = "LMDB_LIB_PATH";
  final private static String PROPERTY_NAME = "lmdb_lib_path";
  
  static Api load() {
    final String prop = System.getProperty(PROPERTY_NAME);
    final String env = System.getenv(ENV_VARIABLE_NAME);
    
    if (null != prop && "" != prop) return tryLoadFromFile("Java Property ("+PROPERTY_NAME+")", prop);
    if (null != env && "" != env) return tryLoadFromFile("Environment Variable ("+ENV_VARIABLE_NAME+")", env);
    
    final Api api = loadFromClasspath();
    
    if (null == api) {
      final String msg = "Unable to load bundled LMDB Library (perhaps it's not included for your OS/CPU combo).  Please specify the path to the LMDB Library in either the "+ENV_VARIABLE_NAME+" environment variable or as a Java System Property via -D"+PROPERTY_NAME+"=/path/to/liblmdb.so";
      logger().log(Level.SEVERE, msg);
      throw new IllegalArgumentException(msg);
    }
    
    return api;
  }
  
  static Api tryLoadFromFile(String source, String filePath) {
    final Path path = Paths.get(filePath);
    
    if (!Files.isRegularFile(path)) {
      final String msg = "Unable to find LMDB Library at: '"+path+"'  Value was found from "+source;
      logger().log(Level.SEVERE, msg);
      throw new IllegalArgumentException(msg);
    } else {
      return load(path.toAbsolutePath().toString());
    }
  }
  
  static Api load(String lib) {
    final Api api = LibraryLoader.create(Api.class).failImmediately().option(LibraryOption.LoadNow, Boolean.TRUE).load(lib);
    logger().log(Level.INFO, "Successfully loaded LMDB library from "+lib);
    return api;
  }
  
  static Api loadFromClasspath() {
    return loadFromClasspath(Loader.class.getClassLoader());
  }
  
  /**
   * Attempt to load the LMDB library from the classpath.
   * <p>
   * If it exists as a regular file then it is directly loaded otherwise it is
   * extracted to and loaded from a temporary file.
   * @param loader the ClassLoader to use
   * @return the Api instance, or null if it was unable to load
   */
  static Api loadFromClasspath(ClassLoader loader) {
    final String path = resourcePath();

    // Attempt to directly use the classpath file if it's a regular file
    try {
      final URL url = loader.getResource(path);
      final Path libPath = Paths.get(url.toURI()).toAbsolutePath();
      if (Files.isRegularFile(libPath)) {
        return load(libPath.toString());
      }
    } catch(Exception ex) {
      // Ignore
    }

    // Extract the lib to a temp file and load that
    try(final InputStream is = loader.getResourceAsStream(path)) {
      if (null == is) return null;
      final Path tmp = Files.createTempFile("lmdb-je", "liblmdb.so");
      Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
      final Api api = load(tmp.toString());
      Files.delete(tmp);
      return api;
    } catch (IOException ex) {
      logger().log(Level.SEVERE, "Caught Exception", ex);
      return null;
    }
  }
  
  static String resourcePath() {
    final Platform platform = Platform.getNativePlatform();
    
    final String os = platform.getOS().name().toLowerCase();
    final String cpu = platform.getCPU().name().toLowerCase();
    
    return "lmdb-je/"+os+"_"+cpu+"/liblmdb.so";
  }
  
  private static Logger logger() {
    return Logger.getLogger(Loader.class.getName());
  }
}
