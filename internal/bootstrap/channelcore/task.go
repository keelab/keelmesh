package channelcore

import (
	"github.com/keelab/keelith/di"
)

func taskModule(inputs ServiceInputs) di.Module {
	//memoryModule := di.MustModule("task.persistence.memory",
	//	di.Value(inputs),
	//	di.Provide(provideMemoryTaskRepository),
	//	di.Provide(provideSequentialTaskID),
	//	di.Export((*domain.TaskRepository)(nil)),
	//	di.Export((*applicationtask.IDGenerator)(nil)),
	//)
	//persistentModule := di.MustModule("task.persistence.postgres-redis",
	//	di.Value(inputs),
	//	di.Provide(providePersistentTaskRepository),
	//	di.Provide(provideUUIDTaskID),
	//	di.Export((*domain.TaskRepository)(nil)),
	//	di.Export((*applicationtask.IDGenerator)(nil)),
	//)
	//return di.MustModule("task",
	//	di.Select(inputs.Config.DependenciesEnabled, persistentModule, memoryModule),
	//	di.Value(inputs),
	//	di.Provide(provideTaskService),
	//	di.Provide(handlertask.New, di.As((*taskv1.TaskServiceKeelithServer)(nil))),
	//	di.Export((*taskv1.TaskServiceKeelithServer)(nil)),
	//)
	return di.Module{}
}
