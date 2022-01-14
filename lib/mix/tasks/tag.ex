defmodule Mix.Tasks.Tag do
  @moduledoc "Tags the current commit with the version specified in the Mix file"
  @shortdoc "Tag the current commit"

  use Mix.Task

  def run(args) do
    # Called just for effect, raise NoProjectError if no project
    Mix.Project.get!()

    project = Mix.Project.config()
    stated  = Version.parse!(project[:version])
    actual  = Version.parse!(git_version())

    case Version.compare(stated, actual) do
      :lt ->
        Mix.Shell.IO.error("Stated version (#{stated}) is less than current commit (#{actual}")
      :eq ->
        Mix.Shell.IO.info("Commit already at v#{stated}")
      :gt ->
        force  = if Enum.member?(args, "--force"), do: "-f ", else: ""
        cmd    = "git tag " <> force <> "-a -m v#{stated} v#{stated}"
        if Enum.member?(args, "--dry-run") do
          Mix.Shell.IO.info("Dry run: #{cmd}")
        else
          Mix.Shell.IO.cmd(cmd)
          Mix.Shell.IO.info("Tagged current commit at v#{stated}")
        end
    end

  end

  defp git_version do
    case System.cmd("git", ~w[describe --dirty=+dirty], stderr_to_stdout: true) do
      {"v" <> version, 0} ->
        version
        |> String.trim
      _ ->
        "0.0.0-dev"
    end
  end
end
