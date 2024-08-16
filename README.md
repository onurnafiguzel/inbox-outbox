public class SendInboxMessageCommandHandler : ICommandHandler<SendInboxMessageCommand>
{
	private readonly IOrderIntegrationInboxRepository _repository;
	private readonly IServiceProvider _serviceProvider;
	private readonly IIntegrationEventToApplicationEventMapper _mapper;
	private readonly ILoggerFacility _logger;
	public SendInboxMessageCommandHandler(IOrderIntegrationInboxRepository repository, IIntegrationEventToApplicationEventMapper mapper, ILoggerFacility logger, IServiceProvider serviceProvider)
	{
		_repository = repository;
		_mapper = mapper;
		_logger = logger;
		_serviceProvider = serviceProvider;
	}
	public async Task HandleAsync(SendInboxMessageCommand command, CancellationToken cancellationToken = default)
	{
		using (var scope = _serviceProvider.CreateScope())
		{
			var eventDispatcher = scope.ServiceProvider.GetService<IEventDispatcher>();
			var commandDispatcher = scope.ServiceProvider.GetService<ICommandDispatcher>();

			const int paginationQueryCount = 250;
			const int transactionCount = 100;
			int errorCount = 0;
			bool isSuccess = false;

			for (int i = 0; i < transactionCount; i++)
			{
				string exceptionMessage = null;
				if (!command.IsError)
					errorCount = 0;

				var messages = await _repository.Get(c => c.ProcessedDate == null && c.IsTryBefore == command.IsError)
					.OrderBy(c => c.OccurredOn).Skip(errorCount).Take(paginationQueryCount).ToListAsync();
				if (!messages.Any())
					break;

				foreach (var item in messages)
				{
					exceptionMessage = null;
					try
					{
						Type type = Assemblies.IntegrationEventAssembly.GetType(item.EventType);
						var integrationEvent = JsonConvert.DeserializeObject(item.EventContent, type) as IIntegrationEvent;

						await _mapper.MapAndSend(integrationEvent, eventDispatcher);
						isSuccess = true;
					}
					catch (HttpRequestException ex)
					{
						var innerException = ex.FindInnerExceptionRecursively();
						exceptionMessage = innerException.Message;
						_logger.Error(nameof(SendInboxMessageCommandHandler), exception: ex);

						isSuccess = true;
						errorCount++;
					}
					catch (Exception ex)
					{
						var innerException = ex.FindInnerExceptionRecursively();
						exceptionMessage = innerException.Message;
						_logger.Error(nameof(SendInboxMessageCommandHandler), exception: ex);

						isSuccess = false;
						errorCount++;
					}
					finally
					{
						await commandDispatcher.SendAsync(new UpdateInboxMessageCommand() 
						{ ExceptionMessage = exceptionMessage, IsSuccess = isSuccess, RecordId = item.Id });
					}
				}
			}
		}
	}

	internal static class Assemblies
	{
		public static readonly Assembly IntegrationEventAssembly = typeof(OrderUpdateIntegrationEvent).Assembly;
		public static readonly Assembly ApplicationEventAssembly = typeof(OrderUpdateClaimApplicationEvent).Assembly;
	}
}
