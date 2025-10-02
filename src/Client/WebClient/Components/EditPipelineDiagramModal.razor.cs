using Microsoft.AspNetCore.Components;
using MudBlazor;

namespace WebClient.Components
{
    public partial class EditPipelineDiagramModal
    {
        [Parameter] public int NodeId { get; set; }
        [Parameter] public string? Value { get; set; }
        [Parameter] public bool CanEdit { get; set; } = false;

        private bool _isEdit = false;
        private MudTextField<string> _textField = default!;

        protected void OnClickEdit()
        {
            _isEdit = true;
        }

        protected void OnClickSave()
        {
            _isEdit = false;
        }
    }
}
