package fr.coding.csvreader.services;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public interface ConfigService {
    default Config load() {
        return ConfigFactory.load();
    }
}
