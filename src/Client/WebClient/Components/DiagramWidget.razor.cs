using Microsoft.AspNetCore.Components;
using MudBlazor;

namespace WebClient.Components
{
    public partial class DiagramWidget
    {
        [Inject] private IDialogService _dialogService { get; set; } = default!;

        protected async Task OnClickEdit()
        {
            var parameters = new DialogParameters();

            parameters.Add("Value", Node.Content);
            parameters.Add("CanEdit", true);

            await _dialogService.ShowAsync<EditPipelineDiagramModal>(Node.Name, parameters, new DialogOptions
            {
                Position = DialogPosition.TopRight,
                MaxWidth = MaxWidth.Small,
                FullWidth = true
            });
        }
    }
}
