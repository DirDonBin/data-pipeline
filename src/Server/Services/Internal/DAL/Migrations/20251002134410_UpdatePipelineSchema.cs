using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Internal.DAL.Migrations
{
    /// <inheritdoc />
    public partial class UpdatePipelineSchema : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "PipelineRunDetails",
                schema: "public");

            migrationBuilder.DropColumn(
                name: "Status",
                schema: "public",
                table: "PipelineRuns");

            migrationBuilder.AddColumn<string>(
                name: "DagFile",
                schema: "public",
                table: "Pipelines",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "Status",
                schema: "public",
                table: "Pipelines",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "ErrorMessage",
                schema: "public",
                table: "PipelineRuns",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "FunctionName",
                schema: "public",
                table: "PipelineFunctions",
                type: "text",
                nullable: false,
                defaultValue: "");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "DagFile",
                schema: "public",
                table: "Pipelines");

            migrationBuilder.DropColumn(
                name: "Status",
                schema: "public",
                table: "Pipelines");

            migrationBuilder.DropColumn(
                name: "ErrorMessage",
                schema: "public",
                table: "PipelineRuns");

            migrationBuilder.DropColumn(
                name: "FunctionName",
                schema: "public",
                table: "PipelineFunctions");

            migrationBuilder.AddColumn<int>(
                name: "Status",
                schema: "public",
                table: "PipelineRuns",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.CreateTable(
                name: "PipelineRunDetails",
                schema: "public",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PipelineRunId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ErrorMessage = table.Column<string>(type: "character varying(5000)", maxLength: 5000, nullable: true),
                    ResultJson = table.Column<string>(type: "text", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PipelineRunDetails", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PipelineRunDetails_PipelineRuns_PipelineRunId",
                        column: x => x.PipelineRunId,
                        principalSchema: "public",
                        principalTable: "PipelineRuns",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_PipelineRunDetails_PipelineRunId",
                schema: "public",
                table: "PipelineRunDetails",
                column: "PipelineRunId",
                unique: true);
        }
    }
}
