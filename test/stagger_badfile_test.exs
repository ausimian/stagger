defmodule StaggerBadFileTest do
  use ExUnit.Case

  @testfile "/tmp/badfile"

  test "corrupt file is truncated" do
    {:ok, file} = :file.open(@testfile, [:raw, :write, :binary])
    :ok = :file.write(file, :crypto.strong_rand_bytes(1234))
    :ok = :file.close(file)
    {:ok, _producer} = Stagger.open(@testfile, hibernate_after: 1000)
  end
end
