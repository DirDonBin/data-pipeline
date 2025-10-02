using Blazor.Diagrams;
using Microsoft.AspNetCore.Components;
using MudBlazor;
using System.Threading.Tasks;
using WebClient.Enums;
using WebClient.Models;

namespace WebClient.Components.Pages
{
    public partial class PipelinePage
    {
        [Inject] private IDialogService _dialogService { get; set; } = default!;

        [Parameter] public int Id { get; set; }

        private PipelineDto _pipeline = new();
        private BlazorDiagram _diagram = null!;
        private int _index { get; set; } = 0;
        private bool _isBlocked => _pipeline.Status == PipelineStatus.Generation || _pipeline.Status == PipelineStatus.Executing;
        private string _statusColor(PipelineStatus status) =>  status switch
        {
            PipelineStatus.Generation or PipelineStatus.Generated or PipelineStatus.Executing => "#DDDDDD",
            PipelineStatus.Fail or PipelineStatus.ErrorOnGeneration => "#E93131",
            PipelineStatus.Success => "#3AA561",
            _ => "#DDDDDD"
        };

        protected override void OnInitialized()
        {
            _diagram = new BlazorDiagram();
        }

        protected async Task OnClickEditDag()
        {
            var parameters = new DialogParameters();

            parameters.Add("Value", _pipeline.Dag);
            parameters.Add("CanEdit", true);

            await _dialogService.ShowAsync<EditPipelineDiagramModal>("DAG", parameters, new DialogOptions
            {
                Position = DialogPosition.TopRight,
                MaxWidth = MaxWidth.Small,
                FullWidth = true
            });
        }

        protected async Task OnClickGenerationDescription()
        {
            var parameters = new DialogParameters();

            parameters.Add("Value", _pipeline.GenerationDescription);

            await _dialogService.ShowAsync<EditPipelineDiagramModal>("Описание генерации пайплайна", parameters, new DialogOptions
            {
                Position = DialogPosition.TopRight,
                MaxWidth = MaxWidth.Small,
                FullWidth = true
            });
        }
    }
}
