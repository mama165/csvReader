package fr.coding.csvreader;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.Directives.*;
import static akka.http.javadsl.server.PathMatchers.longSegment;
import static akka.http.javadsl.server.PathMatchers.segment;

public class CsvReaderService implements ICsvReader {
    private final ActorSystem system;
    private final ActorMaterializer materializer;
    private final String importUrl;
    private final int linesToSkip;
    private final int concurrentFiles;
    private final int concurrentWrites;
    private final int nonIOParallelism;
    private static final Logger logger = LoggerFactory.getLogger(CsvReaderService.class);

    public CsvReaderService(Config config, ActorSystem system) {
        this.system = system;
        this.materializer = ActorMaterializer.create(system);
        this.importUrl = config.getString("importer.import-url");
        this.linesToSkip = config.getInt("importer.lines-to-skip");
        this.concurrentFiles = config.getInt("importer.concurrent-files");
        this.concurrentWrites = config.getInt("importer.concurrent-writes");
        this.nonIOParallelism = config.getInt("importer.non-io-parallelism");
    }

    @Override
    public void importFromHTTPSource(String url) {
        Route route = createRoute();
        Flow<HttpRequest, HttpResponse, NotUsed> handler = route.flow(system, materializer);

        CompletionStage<ServerBinding> binding = Http.get(system).bindAndHandle(handler, ConnectHttp.toHost(importUrl), materializer);

        binding.exceptionally(error -> {
            System.err.println("Something very bad happened! " + error.getMessage());
            system.terminate();
            return null;
        });

        system.terminate();
    }

    private Route createRoute() {
        Flow<ByteString, ByteString, NotUsed> delimiter =
                Framing.delimiter(ByteString.fromString("\n"), 256, FramingTruncation.ALLOW);

        return path(segment("metadata").slash(longSegment()), id ->
                entity(Unmarshaller.entityToMultipartFormData(), formData -> {
                    CompletionStage<Done> done = formData.getParts().mapAsync(1, bodyPart ->
                            bodyPart.getFilename().filter(name -> name.endsWith(".csv")).map(ignored ->
                                    bodyPart.getEntity().getDataBytes()
                                            .via(delimiter)
                                            .map(bs -> bs.utf8String().split(","))
                                            .runForeach(System.out::println, materializer)
                            ).orElseGet(() ->
                                    // in case the uploaded file is not a CSV
                                    CompletableFuture.completedFuture(Done.getInstance()))
                    ).runWith(Sink.ignore(), materializer);
                    // when processing have finished create a response for the user
                    return onComplete(() -> done, ignored -> complete("ok!"));
                }));
    }
}
