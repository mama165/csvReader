package fr.coding.csvreader.services;

import akka.actor.ActorSystem;

public interface ActorSystemService {
    default ActorSystem createSystem() {
        return ActorSystem.create();
    }
}
