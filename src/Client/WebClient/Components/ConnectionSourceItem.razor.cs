using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using MudBlazor;
using WebClient.Models;

namespace WebClient.Components
{
    public partial class ConnectionSourceItem
    {
        [Parameter] public ConnectonSourceDto Info { get; set; } = new();
        [Parameter] public Action? OnDeleteAction { get; set; }

        private IBrowserFile? file = null;
        private bool _expanded = false;
        private List<string> Types =
        [
            "File",
            "PostgreSql", 
            "ClickHouse"
        ];

        private string _expandedBtnIcon => _expanded
            ? Icons.Material.Outlined.KeyboardArrowUp
            : Icons.Material.Outlined.KeyboardArrowDown;

        private void OnExpandCollapseClick()
        {
            _expanded = !_expanded;
        }

        private void OnDeleteBtnClick()
        {
            if (OnDeleteAction != null)
                OnDeleteAction.Invoke();
        }

        private async Task OnFilesChanged()
        {

        }
    }
}
