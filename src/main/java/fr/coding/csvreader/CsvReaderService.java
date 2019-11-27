package fr.coding.csvreader;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.coding.csvreader.balancer.Balancer;
import io.vavr.collection.List;
import io.vavr.control.Try;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.zip.GZIPInputStream;

public class CsvReaderService implements ICsvReader {
    private final ActorSystem system;
    private final File importDirectory;
    private final int linesToSkip;
    private final int concurrentFiles;
    private final int concurrentWrites;
    private final int nonIOParallelism;
    private static final String DELIMITER = "\t";
    private static LoggingAdapter logger;

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create();
        CsvReaderService csvReaderService = new CsvReaderService(ConfigFactory.load(), system);
        csvReaderService.importFromFiles()
                .thenAccept(d -> system.terminate());
    }

    public CsvReaderService(Config config, ActorSystem system) {
        this.system = system;
        this.importDirectory = Paths.get(config.getString("importer.import-directory")).toFile();
        this.linesToSkip = config.getInt("importer.lines-to-skip");
        this.concurrentFiles = config.getInt("importer.concurrent-files");
        this.concurrentWrites = config.getInt("importer.concurrent-writes");
        this.nonIOParallelism = config.getInt("importer.non-io-parallelism");
        logger = Logging.getLogger(system, this);
    }

    public CompletionStage<Done> importFromFiles() {
        List<File> files = List.of(importDirectory.listFiles());

        long startTime = System.currentTimeMillis();

        Graph<FlowShape<File, ValidMovie>, NotUsed> balancer = Balancer.create(concurrentFiles, processSingleFile());

        return Source.from(files)
                .via(balancer)
                .runForeach(this::performLogMovie, ActorMaterializer.create(system))
                .whenComplete((d, e) -> {
                    if (d != null) {
                        logger.info("Import finished in {}s", (System.currentTimeMillis() - startTime) / 1000.0);
                    } else {
                        logger.error("Import failed with this error", e);
                    }
                });
    }

    private void performLogMovie(ValidMovie validMovie) {
        logger.info("tconst : {}", validMovie.getTconst());
    }

    private Flow<File, ValidMovie, NotUsed> processSingleFile() {
        return Flow.of(File.class)
                .via(parseFile())
                .via(filterMovies());
    }

    private Flow<Movie, ValidMovie, NotUsed> filterMovies() {
        return Flow.of(Movie.class)
                .filter(ValidMovie.class::isInstance)
                .map(ValidMovie.class::cast);
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
            Framing.delimiter(ByteString.fromString("\n"), 1064, FramingTruncation.ALLOW);

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
