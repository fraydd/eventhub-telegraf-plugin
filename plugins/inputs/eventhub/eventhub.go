package eventhub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type EventHubInput struct {
	ConnectionString string `toml:"connection_string"`
	EventHubName     string `toml:"eventhub_name"`
	ConsumerGroup    string `toml:"consumer_group"`

	BlobStorageConnectionString string `toml:"blob_storage_connection"`
	CheckpointContainer         string `toml:"checkpoint_container"`

	Log telegraf.Logger `toml:"-"`
}

func (e *EventHubInput) SampleConfig() string {
	return `
  ## Cadena de conexión del Event Hub
  connection_string = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dummyKey;UseDevelopmentEmulator=true;"
  ## Nombre del Event Hub
  eventhub_name = "eh1"
  ## Grupo de consumidores
  consumer_group = "cg1"
`
}

func (e *EventHubInput) Description() string {
	return "Input plugin para leer eventos de Azure Event Hubs"
}

func (plugin *EventHubInput) Init() error {
	if plugin.ConnectionString == "" {
		return fmt.Errorf("ConnectionString cannot be empty")
	}
	if plugin.EventHubName == "" {
		return fmt.Errorf("EventHubName cannot be empty")
	}
	if plugin.ConsumerGroup == "" {
		return fmt.Errorf("ConsumerGroup cannot be empty")
	}
	return nil
}

// Gather lee eventos del Event Hub y los envía a Telegraf
func (e *EventHubInput) Gather(acc telegraf.Accumulator) error {
	// Crear un cliente de consumidor para el Event Hub
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(
		e.ConnectionString,
		e.EventHubName,
		e.ConsumerGroup,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error al crear el cliente de consumidor: %v", err)
	}
	defer consumerClient.Close(context.TODO())
	e.Log.Info("consumerClient creado correctamente")

	// Crear un cliente para el contenedor de blobs (Checkpoint Store)
	checkClient, err := container.NewClientFromConnectionString(
		e.BlobStorageConnectionString,
		e.CheckpointContainer,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error al crear el cliente de blobs: %v", err)
	}
	e.Log.Info("checkClient creado correctamente")

	// Crear un Checkpoint Store
	checkpointStore, err := checkpoints.NewBlobStore(checkClient, nil)
	if err != nil {
		return fmt.Errorf("error al crear el checkpoint store: %v", err)
	}
	e.Log.Info("checkpointStore creado correctamente")

	// Crear un Processor para manejar múltiples particiones
	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		return fmt.Errorf("error al crear el processor: %v", err)
	}
	e.Log.Info("processor creado correctamente")
	dispatchPartitionClients := func(acc telegraf.Accumulator) {
		e.Log.Info("dispatchPartitionClients iniciado")

		for {
			partitionClient := processor.NextPartitionClient(context.TODO())

			if partitionClient == nil {
				break
			}

			go func(pc *azeventhubs.ProcessorPartitionClient) {
				if err := e.processEvents(partitionClient, acc); err != nil {
					panic(err)
				}
			}(partitionClient)
		}
	}

	// run all partition clients
	go dispatchPartitionClients(acc)
	// Ejecutar el Processor
	processorCtx, processorCancel := context.WithCancel(context.TODO())
	defer processorCancel()

	if err := processor.Run(processorCtx); err != nil {
		return fmt.Errorf("error al ejecutar el processor: %v", err)
	}
	return nil
}

func (e *EventHubInput) processEvents(partitionClient *azeventhubs.ProcessorPartitionClient, acc telegraf.Accumulator) error {
	defer closePartitionResources(partitionClient)
	for {
		receiveCtx, receiveCtxCancel := context.WithTimeout(context.TODO(), time.Second)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		receiveCtxCancel()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		e.Log.Infof("Procesando %d evento(s) en la partición %s", len(events), partitionClient.PartitionID())

		// Procesar eventos
		for _, event := range events {
			acc.AddFields("eventhub", map[string]interface{}{
				"message":   string(event.Body),
				"partition": partitionClient.PartitionID(),
				"timestamp": event.EnqueuedTime,
			}, nil)
		}

		if len(events) != 0 {
			if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1], nil); err != nil {
				return err
			}
		}
	}
}

func closePartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	defer partitionClient.Close(context.TODO())
}

// Gather lee eventos del Event Hub y los envía a Telegraf
// func (e *EventHubInput) Gather(acc telegraf.Accumulator) error {
// 	// Crear un cliente de consumidor para el Event Hub
// 	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(
// 		e.ConnectionString,
// 		e.EventHubName,
// 		e.ConsumerGroup,
// 		nil,
// 	)
// 	if err != nil {
// 		return fmt.Errorf("error al crear el cliente de consumidor: %v", err)
// 	}
// 	defer consumerClient.Close(context.TODO())

// 	// Recibir eventos
// 	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
// 	defer cancel()

// 	partitionClient, err := consumerClient.NewPartitionClient("0", nil) // Partición 0
// 	if err != nil {
// 		return fmt.Errorf("error al crear el cliente de partición: %v", err)
// 	}
// 	defer partitionClient.Close(ctx)

// 	events, err := partitionClient.ReceiveEvents(ctx, 100, nil)
// 	if err != nil {
// 		return fmt.Errorf("error al recibir eventos: %v", err)
// 	}

// 	// Procesar eventos
// 	for _, event := range events {
// 		acc.AddFields("eventhub", map[string]interface{}{
// 			"message": string(event.Body),
// 		}, nil)
// 	}

// 	return nil
// }

func init() {
	inputs.Add("eventhub", func() telegraf.Input {
		return &EventHubInput{}
	})
}
