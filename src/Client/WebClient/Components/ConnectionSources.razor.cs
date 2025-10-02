using Microsoft.AspNetCore.Components;
using WebClient.Models;

namespace WebClient.Components
{
    public partial class ConnectionSources
    {
        [Parameter] public List<ConnectonSourceDto> ConnectonItems { get; set; } = [];
        [Parameter] public bool CreateOrExists { get; set; }
        private List<ConnectonSourceDto> _variableConnectonItems { get; set; } = [];

        protected override Task OnInitializedAsync()
        {
            if (CreateOrExists)
            {
                _variableConnectonItems.Add(new ConnectonSourceDto
                {
                    Type = "DataBase",
                    Host = "Test 1"
                });

                _variableConnectonItems.Add(new ConnectonSourceDto
                {
                    Type = "DataBase",
                    Host = "Test 2",
                    Error = "error 404: Test error message"
                });

                _variableConnectonItems.Add(new ConnectonSourceDto
                {

                    Type = "DataBase",
                    Host = "Test 3"
                });
            }

            return base.OnInitializedAsync();
        }

        private void OnClickCreateConnection()
        {
            ConnectonItems.Add(new());
        }

        private void OnDeleteAction(Guid id)
        {
            var current = ConnectonItems.FirstOrDefault(x => x.Id == id);
            if (current != null)
                ConnectonItems.Remove(current);

            StateHasChanged();
        }
    }
}
