import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { McpImportModal } from '../McpImportModal';

const BLOB = JSON.stringify({
  mcpServers: {
    'fuyao-a-share': {
      type: 'http',
      url: 'https://fuyao.aicubes.cn/mcp/a-share',
      headers: { 'X-api-key': '<your-api-key>' },
    },
    'fuyao-meta': {
      type: 'http',
      url: 'https://fuyao.aicubes.cn/mcp/meta',
      headers: { 'X-api-key': '<your-api-key>' },
    },
  },
});

const PLAIN = JSON.stringify({
  mcpServers: { plain: { type: 'http', url: 'https://plain.example.com/mcp' } },
});

// Two vendors' configs that happen to name their server alike and document the
// same spelling. Only the header they want it in differs, so the label says
// which config is on screen.
const VENDOR_A = JSON.stringify({
  mcpServers: {
    acme: { type: 'http', url: 'https://a.example.com/mcp', headers: { 'X-Key': '<your-api-key>' } },
  },
});
const VENDOR_B = JSON.stringify({
  mcpServers: {
    acme: { type: 'http', url: 'https://b.example.com/mcp', headers: { 'X-Token': '<your-api-key>' } },
  },
});

const result = (names: string[]) => ({
  results: names.map((name) => ({ name, original_name: name, renamed: false, status: 'created' as const })),
  created: names.length,
  secrets_created: [],
  config_version: 2,
});

beforeEach(() => vi.clearAllMocks());

describe('McpImportModal: placeholder credentials', () => {
  it('asks each server for its own value and sends neither key to the other', async () => {
    const onImport = vi.fn().mockResolvedValue(result(['fuyao_a_share', 'fuyao_meta']));
    render(<McpImportModal onClose={vi.fn()} onImport={onImport} />);

    fireEvent.change(screen.getByTestId('mcp-import-text'), { target: { value: BLOB } });
    expect(await screen.findByRole('button', { name: /^next$/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /^next$/i }));

    // Two vendors documenting the same spelling are two credentials, and the
    // field is named by where its value goes rather than by the config's text.
    const step = screen.getByTestId('mcp-import-credentials');
    expect(step).toHaveTextContent('fuyao_a_share · X-api-key');
    expect(step).toHaveTextContent('fuyao_meta · X-api-key');
    expect(step).not.toHaveTextContent('<your-api-key>');

    const fields = screen.getAllByPlaceholderText(/paste the real value/i);
    expect(fields).toHaveLength(2);
    fireEvent.change(fields[0], { target: { value: 'key-a' } });
    fireEvent.change(fields[1], { target: { value: 'key-b' } });
    fireEvent.click(screen.getByRole('button', { name: /^import 2$/i }));

    await waitFor(() => expect(onImport).toHaveBeenCalledTimes(1));
    const sent = onImport.mock.calls[0][0] as { mcpServers: Record<string, { headers: Record<string, string> }> };
    expect(sent.mcpServers['fuyao-a-share'].headers['X-api-key']).toBe('key-a');
    expect(sent.mcpServers['fuyao-meta'].headers['X-api-key']).toBe('key-b');
  });

  it('leaves the server the user skipped holding its placeholder', async () => {
    const onImport = vi.fn().mockResolvedValue(result(['fuyao_a_share', 'fuyao_meta']));
    render(<McpImportModal onClose={vi.fn()} onImport={onImport} />);
    fireEvent.change(screen.getByTestId('mcp-import-text'), { target: { value: BLOB } });
    fireEvent.click(await screen.findByRole('button', { name: /^next$/i }));

    fireEvent.change(screen.getAllByPlaceholderText(/paste the real value/i)[0], {
      target: { value: 'key-a' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^import 2$/i }));

    await waitFor(() => expect(onImport).toHaveBeenCalledTimes(1));
    const sent = onImport.mock.calls[0][0] as { mcpServers: Record<string, { headers: Record<string, string> }> };
    expect(sent.mcpServers['fuyao-a-share'].headers['X-api-key']).toBe('key-a');
    expect(sent.mcpServers['fuyao-meta'].headers['X-api-key']).toBe('<your-api-key>');
  });

  it('imports the placeholder as-is when the field is left blank', async () => {
    const onImport = vi.fn().mockResolvedValue(result(['fuyao_a_share', 'fuyao_meta']));
    render(<McpImportModal onClose={vi.fn()} onImport={onImport} />);
    fireEvent.change(screen.getByTestId('mcp-import-text'), { target: { value: BLOB } });
    fireEvent.click(await screen.findByRole('button', { name: /^next$/i }));
    fireEvent.click(screen.getByRole('button', { name: /^import 2$/i }));
    await waitFor(() => expect(onImport).toHaveBeenCalledTimes(1));
    const sent = onImport.mock.calls[0][0] as { mcpServers: Record<string, { headers: Record<string, string> }> };
    expect(sent.mcpServers['fuyao-meta'].headers['X-api-key']).toBe('<your-api-key>');
  });

  it('forgets the answers when a different config is pasted over the last one', async () => {
    const onImport = vi.fn().mockResolvedValue(result(['acme']));
    render(<McpImportModal onClose={vi.fn()} onImport={onImport} />);

    fireEvent.change(screen.getByTestId('mcp-import-text'), { target: { value: VENDOR_A } });
    fireEvent.click(await screen.findByRole('button', { name: /^next$/i }));
    await screen.findByText('acme · X-Key');
    fireEvent.change(screen.getByPlaceholderText(/paste the real value/i), {
      target: { value: 'key-a' },
    });

    fireEvent.click(screen.getByRole('button', { name: /^back$/i }));
    fireEvent.change(screen.getByTestId('mcp-import-text'), { target: { value: VENDOR_B } });
    fireEvent.click(await screen.findByRole('button', { name: /^next$/i }));
    await screen.findByText('acme · X-Token');

    // Same server name, same spelling, a different vendor: the field starts
    // blank, and importing from here sends nothing the user typed for the
    // config they replaced.
    expect(screen.getByPlaceholderText(/paste the real value/i)).toHaveValue('');
    fireEvent.click(screen.getByRole('button', { name: /^import 1$/i }));
    await waitFor(() => expect(onImport).toHaveBeenCalledTimes(1));
    const sent = onImport.mock.calls[0][0] as {
      mcpServers: Record<string, { headers: Record<string, string> }>;
    };
    expect(sent.mcpServers.acme.headers['X-Token']).toBe('<your-api-key>');
  });

  it('skips the credentials step when nothing is a placeholder', async () => {
    const onImport = vi.fn().mockResolvedValue(result(['plain']));
    render(<McpImportModal onClose={vi.fn()} onImport={onImport} />);
    fireEvent.change(screen.getByTestId('mcp-import-text'), { target: { value: PLAIN } });
    fireEvent.click(await screen.findByRole('button', { name: /^import 1$/i }));
    await waitFor(() => expect(onImport).toHaveBeenCalledTimes(1));
    expect(screen.queryByTestId('mcp-import-credentials')).not.toBeInTheDocument();
  });
});

describe('McpImportModal: switching imports on', () => {
  it('switches every created row on through onEnable and marks each as it lands', async () => {
    const onImport = vi.fn().mockResolvedValue(result(['a', 'b']));
    const onEnable = vi.fn().mockResolvedValue(undefined);
    render(<McpImportModal onClose={vi.fn()} onImport={onImport} onEnable={onEnable} />);
    fireEvent.change(screen.getByTestId('mcp-import-text'), {
      target: { value: JSON.stringify({ mcpServers: { a: { url: 'https://a.example.com/mcp' }, b: { url: 'https://b.example.com/mcp' } } }) },
    });
    fireEvent.click(await screen.findByRole('button', { name: /^import 2$/i }));
    fireEvent.click(await screen.findByTestId('mcp-import-enable-all'));
    await waitFor(() => expect(onEnable).toHaveBeenCalledTimes(2));
    expect(onEnable.mock.calls.map((c) => c[0])).toEqual(['a', 'b']);
    expect(await screen.findAllByText('On')).toHaveLength(2);
  });

  it('offers nothing to switch on where the caller has no onEnable', async () => {
    const onImport = vi.fn().mockResolvedValue(result(['plain']));
    render(<McpImportModal onClose={vi.fn()} onImport={onImport} />);
    fireEvent.change(screen.getByTestId('mcp-import-text'), { target: { value: PLAIN } });
    fireEvent.click(await screen.findByRole('button', { name: /^import 1$/i }));
    await screen.findByText(/imported 1 of 1/i);
    expect(screen.queryByTestId('mcp-import-enable-plain')).not.toBeInTheDocument();
  });
});
