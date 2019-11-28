package fr.coding.csvreader.services;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.function.Predicate;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.coding.csvreader.balancer.Balancer;
import fr.coding.csvreader.features.ICsvReader;
import fr.coding.csvreader.models.InvalidMovie;
import fr.coding.csvreader.models.Movie;
import fr.coding.csvreader.models.ValidMovie;
import fr.coding.csvreader.repository.MessageQueueRepository;
import io.vavr.collection.List;
import io.vavr.control.Try;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.zip.GZIPInputStream;

public final class CsvImportService implements ICsvReader {
    private final ActorSystemService actorSystemService;
    private final MessageQueueRepository messageQueueRepository;
    private final File importDirectory;
    private final int linesToSkip;
    private final int concurrentFiles;
    private final int concurrentWrites;
    private final int nonIOParallelism;
    private static final String DELIMITER = "\t";
    private static final int MAXIMUM_FRAME_LENGTH = 1064;
    private static LoggingAdapter logger;

    public static void main(String[] args) {
//        CsvImportService csvImportService = new CsvImportService(ConfigService, ActorSystem.create(), new MessageQueueRepositoryImpl());
//        csvImportService.readCsv("movie", "Comedy");
    }

    public CsvImportService(ConfigService configService, ActorSystemService actorSystemService, MessageQueueRepository messageQueueRepository) {
        this.actorSystemService = actorSystemService;
        this.messageQueueRepository = messageQueueRepository;
        this.importDirectory = Paths.get(getConfig(configService).getString("importer.import-directory")).toFile();
        this.linesToSkip = getConfig(configService).getInt("importer.lines-to-skip");
        this.concurrentFiles = getConfig(configService).getInt("importer.concurrent-files");
        this.concurrentWrites = getConfig(configService).getInt("importer.concurrent-writes");
        this.nonIOParallelism = getConfig(configService).getInt("importer.non-io-parallelism");
        logger = Logging.getLogger(actorSystemService.createSystem(), CsvImportService.class);
    }

    private Config getConfig(ConfigService configService) {
        return configService.load();
    }

    @Override
    public void readCsv(String titleType, String gender) {
        ActorSystem actorSystem = actorSystemService.createSystem();

        importFromFiles(titleType, gender)
                .thenAccept(d -> {
                    actorSystem.terminate();
                });
    }

    private CompletionStage<Done> importFromFiles(String titleType, String gender) {
        List<File> filteredFiles = filterFilesWithExtension(importDirectory.listFiles(), ".gz");
        ActorMaterializer materializer = ActorMaterializer.create(actorSystemService.createSystem());

        long startTime = System.currentTimeMillis();

        Graph<FlowShape<File, ValidMovie>, NotUsed> balancer = Balancer.create(concurrentFiles, processSingleFile(titleType, gender));

        return Source.from(filteredFiles)
                .via(balancer)
                .alsoTo(Sink.foreach(this::performLogMovie))
                .runWith(storeMovie(), materializer)
                .whenComplete((d, e) -> {
                    if (d != null) {
                        logger.info("Import finished in {}s", (System.currentTimeMillis() - startTime) / 1000.0);
                    } else {
                        logger.error("Import failed with this error :  {}", e);
                    }
                });
    }

    private Sink<ValidMovie, CompletionStage<Done>> storeMovie() {
        return Flow.of(ValidMovie.class)
                .mapAsyncUnordered(concurrentWrites, messageQueueRepository::publish)
                .toMat(Sink.ignore(), Keep.right());
    }

    private List<File> filterFilesWithExtension(File[] files, String extension) {
        return List.of(files)
                .filter(file -> file != null && file.getName().endsWith(extension))
                .collect(List.collector());
    }

    private void performLogMovie(ValidMovie validMovie) {
        logger.info("{}   {}   {}", validMovie.getTconst(), validMovie.getTitleType(), validMovie.getGenders());
    }

    private Flow<File, ValidMovie, NotUsed> processSingleFile(String titleType, String gender) {
        return Flow.of(File.class)
                .via(parseFile())
                .via(filterMovies(titleType, gender));
    }

    private Flow<Movie, ValidMovie, NotUsed> filterMovies(String titleType, String gender) {
        Predicate<ValidMovie> PredicateFilterWithCriteria = movie ->
                movie != null
                        && titleType.equals(movie.getTitleType())
                        && movie.matchGender(gender);

        return Flow.of(Movie.class)
                .filter(ValidMovie.class::isInstance)
                .map(ValidMovie.class::cast)
                .filter(PredicateFilterWithCriteria);
    }

    private Flow<File, Movie, NotUsed> parseFile() {
        return Flow.of(File.class).flatMapConcat(file -> {
            GZIPInputStream inputStream = new GZIPInputStream(new FileInputStream(file));
            return StreamConverters.fromInputStream(() -> inputStream)
                    .via(lineDelimiter)
                    .drop(linesToSkip)
                    .map(ByteString::utf8String)
                    .mapAsync(nonIOParallelism, this::parseLine);
        });
    }

    private Flow<ByteString, ByteString, NotUsed> lineDelimiter =
            Framing.delimiter(ByteString.fromString("\n"), MAXIMUM_FRAME_LENGTH, FramingTruncation.ALLOW);

    private CompletionStage<Movie> parseLine(String line) {
        return CompletableFuture.supplyAsync(() -> {
            List<String> separatedValues = List.ofAll(Arrays.asList(line.split(DELIMITER)));

            return Try
                    .of(() -> (Movie) ValidMovie.create(separatedValues))
                    .onFailure(e -> logger.error("Unable to parse movie: {}: | error : {}", line, e.getMessage()))
                    .getOrElse(new InvalidMovie(separatedValues.get(0)));
        });
    }
}
