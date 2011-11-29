//
// Groovy script to scan for photo files on disk and keep track of where they
// are and if you have them in a "master" location.
//
// This is under development...
//
// The idea is:
//
//  (1) Run the script to record all the photo files currently in your "master" collection. In my case, this is
//      an Apple Aperture library (which actually holds all the master files in hidden sub-folders). The script
//      does an MD5 sum of the file and creates a record in a CouchDB database for each file and flags it as
//      being in the master collection. The command would be something like this:
//
//      groovy scan-files.groovy -u -m -v -s /Volumes/MyDisk/Pictures/MyApertureLibrary.aplibrary
//
//  (2) Do the above as many times as you like for each master collection or following changes
//
//  (3) Now you have some random disk you threw some photos onto 8 months ago and you want to know if the files
//      are in the master collection. You can run the command like this to find out:
//
//      groovy scan-files.groovy -n -s /Volumes/MyOtherDisk/Pictures/SomeFolder
//
//  (4) Record all files on a disk into the DB for tracking
//
//      groovy scan-files.groovy -u -v -s /Volumes/MyDisk/
//
//  (5) Find out the status of a given file:
//
//      groovy scan-files.groovy -v -r -n -s /Volumes/MyDisk/Folder/photo-file.jpg
//
//  The DB holds a record by MD5 of the photo file recording all paths where it was found and whether
//  it was found in a master location.
//
//  What is required:
//
//  - Java6 or later
//  - Groovy (http://groovy.codehaus.org/)
//  - CouchDB (See http://couchdb.apache.org/ and get community binaries from http://www.couchbase.com/downloads)
//
// TO DO:
//
// - Output to log file option
// - Copy not found files to specified dir option
// - CouchDB connection options
// - CouchDB useful views (e.g. duplicates)
// - File size
// - Record EXIF in DB
// - Queries off DB (find EXIF, size, duplicates)
// - Removed from master location / rescan master (needs to remove master flag from records, currently sticks)
//
@Grapes([
    @Grab(group='org.ektorp', module='org.ektorp', version='1.2.2')

])


// For MD5
import java.security.MessageDigest

// For CouchDB persistence
import org.ektorp.*;
import org.ektorp.impl.*;
import org.ektorp.http.*
import org.codehaus.jackson.annotate.*

// Pre-declares
def photoDB
def opt

//
// Util to MD5 sum a file - so can find dupes
//
def generateMD5(final file) {
   MessageDigest digest = MessageDigest.getInstance("MD5")
   file.withInputStream(){is->
   byte[] buffer = new byte[8192]
   int read = 0
      while( (read = is.read(buffer)) > 0) {
             digest.update(buffer, 0, read);
         }
     }
   byte[] md5sum = digest.digest()
   BigInteger bigInt = new BigInteger(1, md5sum)
   return bigInt.toString(16)
}

// Class for found file - persisting this to CouchDB
class PhotoFile
{
    // To start, I am going to make the ID the MD5
    @JsonProperty("_id")
    String _id

    @JsonProperty("_rev")
    String _rev

    // MD5 sum of the whole photo file
    String md5

    // If it is in the master library
    String master
    String masterPath

    // List of paths found on
    List    foundPaths

    // First entered
    Date firstSeen
    Date lastSeen

    // Last place found
    String lastSeenPath

    void printDetails() {
        println "-------------------------------------------------------------------------------------------------------"
        println "_id:        $_id"
        println "_rev:       $_rev"
        println "md5:        $md5"
        println "master:     $master"
        println "masterPath: $masterPath"
        println "foundPaths:"
        foundPaths.each {
            println "            $it"
        }
        println "firstSeen:  $firstSeen"
        println "lastSeen:   $lastSeen"
        println "-------------------------------------------------------------------------------------------------------"
    }
}

// Look for existing entry in DB by MD5
def FindExisting = { md5 ->

   def photo = null
   try {
        photo = photoDB.get(PhotoFile.class, md5)
   }
   catch(org.ektorp.DocumentNotFoundException e) {}

   return photo
}

// Create an entry in the DB
def CreateOrUpdate = { md5, foundPath ->

    // Look for existing
    def photo = FindExisting(md5)
    if(photo == null)
    {
        photo = new PhotoFile()
        photo._id = md5
        photo.md5 = md5
        photo.firstSeen = new Date()
        photo.lastSeen = photo.firstSeen
        photo.lastSeenPath = foundPath
        photo.foundPaths = [foundPath]
        photo.master = opt.m ? "1" : "0"
        photo.masterPath = opt.m ? foundPath : ""

        if(opt.u) photoDB.create(photo)
    }
    else
    {
        photo.lastSeen = new Date()
        if(!photo.foundPaths.find { it == foundPath})
            photo.foundPaths.add(foundPath)

        if(opt.m && photo.master == "0")
        {
            photo.master = "1"
            photo.masterPath = foundPath
        }

        if(opt.u) photoDB.update(photo)
    }

    return photo
}

// Command line
def cli = new CliBuilder()
cli.s(longOpt:"sourceFiles", args:1, required:true, "The folder to scan files in")
cli.m(longOpt:"setMaster", "Set any found files as in the master DB (scanning master location)")
cli.r(longOpt:"reportMaster", "Report files in master location")
cli.n(longOpt:"reportNotMaster", "Report files NOT in master location")
cli.u(longOpt:"update", "Update DB")
cli.e(longOpt:"fileExtension",args:1, "Search for files of defined file extension(s) - comma separated")
cli.v(longOpt:"verbose", "Verbose output")
cli.p(longOpt:"port",args:1, "Port for CouchDb connection")
opt = cli.parse(args)
if(!opt) return(1)

// Connect / create the DB
def  httpClient = new StdHttpClient.Builder().url("http://localhost:${opt.p?:5984}").build()
def  dbInstance = new StdCouchDbInstance(httpClient)
photoDB = dbInstance.createConnector("photo-info", true)

// File search pattern. We can over-ride extensions on the command line
def pat = ~/([^\s]+(\.(?i)(jpg|orf|tif|tiff|mov|avi))$)/
if(opt.e)
    pat = ~/([^\s]+(\.(?i)(${opt.e.toString().replace(',','|')}))$)/

// Some stats to keep
def filesTotal = 0
def mastersFound = 0
def nonMastersFound = 0

// Process a single file
def doFile = { filePath ->

    filesTotal++

    def md5 = generateMD5(new File("${filePath}"))

    if(opt.v)
        println "md5: ${md5}, File: ${filePath}"

    def photo = CreateOrUpdate(md5, filePath)

    if(opt.v)
        photo.printDetails()

    if(photo.master == "1")
    {
        mastersFound++
        if (opt.r) println "InMaster: $filePath"
    }
    if(photo.master == "0")
    {
        nonMastersFound++
        if (opt.n) println "NotInMaster: $filePath"
    }
}

// Do a source folder passed in, recursively
def fileScanSource
fileScanSource = { f ->

    f.eachDir( fileScanSource )

    f.eachFileMatch(pat) {

        doFile(it.canonicalPath)
    }
}

// Handle supplied source file system object (dir or file)
if(opt.s)
{
    def source = new File(opt.s)

    if(source.isDirectory()) {
        fileScanSource( source )
    }
    if(source.isFile()) {
        doFile(opt.s)
    }
}

println "-------------------------------------------------------------------------------------------------------"
println "Done"
println "Total files: $filesTotal"
println "Files in master location: $mastersFound"
println "Files NOT in master location: $nonMastersFound"
println "-------------------------------------------------------------------------------------------------------"
