package fr.coding.csvreader.repository;

import akka.Done;
import fr.coding.csvreader.models.Movie;

import java.util.concurrent.CompletionStage;

public interface MessageQueueRepository {
    CompletionStage<Done> publish(Movie movie);
}
