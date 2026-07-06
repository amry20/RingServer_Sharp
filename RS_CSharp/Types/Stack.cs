/***************************************************************************
 * Stack.cs
 *
 * Stack data structure with doubly-linked list implementation.
 *
 * This file is part of the ringserver C# port.
 *
 * Original C code by Emin Marinian / Chad Trabant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 ***************************************************************************/

using System;
using System.Collections.Generic;

namespace RingServer.Types;

/// <summary>
/// Simple stack implementation with push, pop, shift, unshift operations.
/// Equivalent to Stack in stack.h (doubly-linked list).
/// </summary>
public class Stack<T>
{
    private readonly LinkedList<T> _list = new();

    /// <summary>
    /// Create a new empty stack
    /// </summary>
    public static Stack<T> Create()
    {
        return new Stack<T>();
    }

    /// <summary>
    /// Push an item onto the top of the stack
    /// </summary>
    public void Push(T item)
    {
        _list.AddLast(item);
    }

    /// <summary>
    /// Pop an item from the top of the stack
    /// </summary>
    public T? Pop()
    {
        if (_list.Count == 0)
            return default;
        var item = _list.Last!.Value;
        _list.RemoveLast();
        return item;
    }

    /// <summary>
    /// Add an item to the front of the stack (unshift)
    /// </summary>
    public void Unshift(T item)
    {
        _list.AddFirst(item);
    }

    /// <summary>
    /// Remove and return an item from the front of the stack (shift)
    /// </summary>
    public T? Shift()
    {
        if (_list.Count == 0)
            return default;
        var item = _list.First!.Value;
        _list.RemoveFirst();
        return item;
    }

    /// <summary>
    /// Check if the stack is not empty
    /// </summary>
    public bool NotEmpty => _list.Count > 0;

    /// <summary>
    /// Number of items in the stack
    /// </summary>
    public int Count => _list.Count;

    /// <summary>
    /// Peek at the top item without removing it
    /// </summary>
    public T? Peek()
    {
        if (_list.Count == 0)
            return default;
        return _list.Last!.Value;
    }

    /// <summary>
    /// Convert to an array
    /// </summary>
    public T[] ToArray()
    {
        var result = new T[_list.Count];
        _list.CopyTo(result, 0);
        return result;
    }

    /// <summary>
    /// Sort the stack using a comparison function
    /// </summary>
    public void Sort(Comparison<T> comparison)
    {
        var array = ToArray();
        Array.Sort(array, comparison);
        _list.Clear();
        foreach (var item in array)
        {
            _list.AddLast(item);
        }
    }

    /// <summary>
    /// Join two stacks (destructive: stack2 is consumed)
    /// </summary>
    public static Stack<T> Join(Stack<T> stack1, Stack<T> stack2)
    {
        var result = new Stack<T>();
        foreach (var item in stack1._list)
            result._list.AddLast(item);
        foreach (var item in stack2._list)
            result._list.AddLast(item);
        return result;
    }
}

/// <summary>
/// Non-generic Stack alias for compatibility with original C interface
/// </summary>
public class Stack : Stack<object>
{
}