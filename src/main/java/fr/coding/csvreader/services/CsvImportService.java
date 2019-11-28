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
    private final ActorSystem system;
    private final MessageQueueRepository messageQueueRepository;
    private final File importDirectory;
    private final int linesToSkip;
    private final int concurrentFiles;
    private final int concurrentWrites;
    private final int nonIOParallelism;
    private static final String DELIMITER = "\t";
    private static final int MAXIMUM_FRAME_LENGTH = 1064;
    private static LoggingAdapter logger;

    public CsvImportService(Config config, ActorSystem system, MessageQueueRepository messageQueueRepository) {
        this.system = system;
        this.messageQueueRepository = messageQueueRepository;
        this.importDirectory = Paths.get(config.getString("importer.import-directory")).toFile();
        this.linesToSkip = config.getInt("importer.lines-to-skip");
        this.concurrentFiles = config.getInt("importer.concurrent-files");
        this.concurrentWrites = config.getInt("importer.concurrent-writes");
        this.nonIOParallelism = config.getInt("importer.non-io-parallelism");
        logger = Logging.getLogger(system, CsvImportService.class);
    }

    @Override
    public void readCsv(String titleType, String gender) {
        importFromFiles(titleType, gender)
                .thenAccept(d -> system.terminate());
    }

    private CompletionStage<Done> importFromFiles(String titleType, String gender) {
        List<File> filteredFiles = filterFilesWithExtension(importDirectory.listFiles(), ".gz");

        long startTime = System.currentTimeMillis();

        Graph<FlowShape<File, ValidMovie>, NotUsed> balancer = Balancer.create(concurrentFiles, processSingleFile(titleType, gender));

        return Source.from(filteredFiles)
                .via(balancer)
                .runForeach(this::performLogMovie, ActorMaterializer.create(system))

                .whenComplete((d, e) -> {
                    if (d != null) {
                        logger.info("Import finished in {}s", (System.currentTimeMillis() - startTime) / 1000.0);
                    } else {
                        logger.error("Import failed with this error :  {}", e);
                    }
                });
    }

    private Sink<ValidMovie, CompletionStage<Done>> egzrgerg() {
        return null;
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
