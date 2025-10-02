using DataPipeline.ApiClient.Internal;
using Microsoft.AspNetCore.Components;
using WebClient.Models;

namespace WebClient.Components.Pages
{
    public partial class PiplineEditPage
    {
        [Inject] private IInternalApiClient _internalApi { get; set; } = default!;

        [Parameter] public int? Id { get; set; }
        [Parameter] public string Name { get; set; } = "Новый Pipeline";

        private List<ConnectonSourceDto> _connectonItems { get; set; } = [];
        private List<ConnectonTargetDto> _connectonTargetItems { get; set; } = [];

        protected override Task OnInitializedAsync()
        {
            OnInitData();

            return base.OnInitializedAsync();
        }

        private void OnInitData()
        {
            if (!Id.HasValue)
                return;

            Name = "Pipeline #" + Id.Value;
        }

        private async Task OnClickSave()
        {
            await _internalApi.CreatePipelineAsync(new CreatePipelineDto
            {
                Name = Name,
                Sources = _connectonItems.Select(x =>
                {
                    var type = x.Type == "File" ?
                        DataNodeType.File
                            : x.Type == DataNodeType.ClickHouse.ToString()
                                ? DataNodeType.ClickHouse
                                    : DataNodeType.PostgreSql;


                    return new DataNodeDto
                    {
                        DataNodeType = type,
                        DatabaseConfig = type == DataNodeType.File ? null
                        : new DatabaseNodeConfigDto
                        {
                            Database = x.DataBase,
                            Host = x.Host,
                            Port = x.Port,
                            DatabaseType = type == DataNodeType.PostgreSql ? DatabaseType.PostgreSql : DatabaseType.ClickHouse,
                            Password = x.Password,
                            Username = x.Login
                        },
                        Name = type.ToString(),
                        FileConfig = new FileNodeConfigDto
                        {
                            FileUrl = x.FileUrl,
                            FileType = x.FileType
                        }

                    };
                }).ToList(),
                Targets = _connectonTargetItems.Select(x =>
                {
                    var type = x.Type == "File" ?
                        DataNodeType.File
                            : x.Type == DataNodeType.ClickHouse.ToString()
                                ? DataNodeType.ClickHouse
                                    : DataNodeType.PostgreSql;


                    return new DataNodeDto
                    {
                        DataNodeType = type,
                        DatabaseConfig = type == DataNodeType.File ? null
                        : new DatabaseNodeConfigDto
                        {
                            Database = x.DataBase,
                            Host = x.Host,
                            Port = x.Port,
                            DatabaseType = type == DataNodeType.PostgreSql ? DatabaseType.PostgreSql : DatabaseType.ClickHouse,
                            Password = x.Password,
                            Username = x.Login
                        },
                        Name = type.ToString()
                    };
                }).ToList(),
            });
        }
    }
}
