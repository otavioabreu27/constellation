import constellation/domains/buffer

pub fn batches_are_buffered_in_fifo_order_test() {
  let value =
    buffer.new()
    |> buffer.push([1, 2])
    |> buffer.push([3, 4])

  assert buffer.to_list(value) == [1, 2, 3, 4]
}

pub fn buffer_built_from_list_preserves_order_test() {
  let value = buffer.from_list([1, 2, 3])

  assert buffer.to_list(value) == [1, 2, 3]
}
