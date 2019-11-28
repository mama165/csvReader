package fr.coding.csvreader.implementations;

import akka.Done;
import fr.coding.csvreader.models.Movie;
import fr.coding.csvreader.repository.MessageQueueRepository;

import java.util.concurrent.CompletionStage;

public class MessageQueueRepositoryImpl implements MessageQueueRepository {
    @Override
    public CompletionStage<Done> publish(Movie movie) {
        return null;
    }
}
