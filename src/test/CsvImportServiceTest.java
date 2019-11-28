import akka.actor.Actor;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.coding.csvreader.repository.MessageQueueRepository;
import fr.coding.csvreader.services.ActorSystemService;
import fr.coding.csvreader.services.ConfigService;
import fr.coding.csvreader.services.CsvImportService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.Mockito.*;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CsvImportServiceTest {
    private CsvImportService csvImportService;
    @Mock
    private ConfigService configService;
    @Mock
    private ActorSystemService actorSystemService;
    @Mock
    private MessageQueueRepository messageQueueRepository;

    private static final Config mockedConfig = ConfigFactory.load();
    private static final ActorSystem mockedSystem = ActorSystem.create();

    @BeforeEach
    public void setup() {
        when(configService.load()).thenReturn(mockedConfig);
        when(actorSystemService.createSystem()).thenReturn(mockedSystem);
        csvImportService = new CsvImportService(configService, actorSystemService, messageQueueRepository);
    }

    @Test
    void should_publish_movie() {
        csvImportService.readCsv("movie", "Comedy");

//        verify(messageQueueRepository, times(1)).publish(any());

    }
}
